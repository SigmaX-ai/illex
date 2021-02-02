// Copyright 2020 Teratide B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "illex/client_buffering.h"

#include <string.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>
#include <vector>

#include "illex/latency.h"
#include "illex/log.h"
#include "illex/status.h"

namespace illex {

auto JSONBuffer::Create(std::byte* buffer, size_t capacity, JSONBuffer* out) -> Status {
  if (buffer == nullptr) {
    return Status(Error::ClientError, "Pre-allocated buffer cannot be nullptr.");
  }
  if (capacity == 0) {
    return Status(Error::ClientError, "Size cannot be 0.");
  }
  *out = JSONBuffer(buffer, capacity);

  return Status::OK();
}

auto JSONBuffer::SetSize(size_t size) -> Status {
  if (size <= capacity_) {
    size_ = size;
    return Status::OK();
  } else {
    return Status(Error::ClientError,
                  "Cannot set buffer size larger than allocated capacity.");
  }
}

auto BufferingClient::Create(const ClientOptions& options,
                             const std::vector<JSONBuffer*>& buffers,
                             const std::vector<std::mutex*>& mutexes,
                             BufferingClient* out) -> Status {
  // Sanity check.
  // TODO: we could also get a vector of a buffer-mutex pair
  if (mutexes.size() != buffers.size()) {
    return Status(Error::ClientError,
                  "Cannot create client. "
                  "Number of buffers mismatches number of mutexes.");
  }
  out->mutexes = mutexes;
  out->buffers = buffers;
  out->seq = options.seq;
  // Create an endpoint.
  auto endpoint = options.host + ":" + std::to_string(options.port);
  out->client = std::make_shared<Socket>(kissnet::endpoint(endpoint));
  // Attempt to connect.
  SPDLOG_DEBUG("Client connecting to {}...", endpoint);
  auto success = out->client->connect();
  if (!success) {
    return Status(Error::ClientError, "Unable to connect to server.");
  }
  // Connect successful, remember we have to close it on deconstruction.
  out->must_be_closed = true;

  return Status::OK();
}

auto JSONBuffer::Scan(size_t num_bytes, uint64_t seq) -> std::pair<size_t, size_t> {
  size_t first = seq;
  size_t num_jsons = 0;
  // Scan the buffer for a newline.
  const auto* recv_chars = reinterpret_cast<const char*>(this->data());
  const char* json_start = recv_chars;
  const char* json_end = recv_chars + this->size();
  size_t remaining = num_bytes;
  do {
    json_end = static_cast<const char*>(std::memchr(json_start, '\n', remaining));
    if (json_end != nullptr) {
      auto json_len = json_end - json_start;
      if (json_len > 0) {
        num_jsons++;
      }
      // Move the start scan index to the character after the newline.
      // But only if the json end is not the last character.
      if (json_start != json_end - 1) {
        json_start = json_end + 1;
      }
      // Calculate the remaining number of bytes to scan in the buffer.
      remaining = (recv_chars + num_bytes) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while ((json_end != nullptr) && (json_start != json_end));

  // Set contained sequence numbers.
  SetRange({first, first + num_jsons - 1});

  // Return number of JSONs and number of remaining bytes.
  return {num_jsons, remaining};
}

void JSONBuffer::Reset() {
  size_ = 0;
  seq_range = {0, 0};
}

auto JSONBuffer::num_jsons() const -> size_t {
  return seq_range.last - seq_range.first + 1;
}

auto TryGetEmptyBuffer(const std::vector<JSONBuffer*>& buffers,
                       const std::vector<std::mutex*>& mutexes, JSONBuffer** out,
                       size_t* lock_idx) -> bool {
  const size_t num_buffers = buffers.size();
  for (size_t i = 0; i < num_buffers; i++) {
    if (buffers[i]->empty()) {
      if (mutexes[i]->try_lock()) {
        *lock_idx = i;
        *out = buffers[i];
        return true;
      }
    }
  }
  *out = nullptr;
  return false;
}

auto BufferingClient::ReceiveJSONs(LatencyTracker* lat_tracker) -> Status {
  // Buffer to temporary place leftover bytes from a buffer.
  auto* spill = static_cast<std::byte*>(std::malloc(ILLEX_DEFAULT_TCP_BUFSIZE));
  // Bytes leftover from previous buffer.
  size_t remaining = 0;

  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to get a lock on an empty buffer.
      JSONBuffer* buf = nullptr;
      size_t lock_idx = 0;
      if (TryGetEmptyBuffer(buffers, mutexes, &buf, &lock_idx)) {
        // Copy leftovers from previous buffer into new buffer.
        if (remaining > 0) {
          std::memcpy(buf->mutable_data(), spill, remaining);
        }

        // Attempt to receive some bytes.
        auto recv_status =
            client->recv(buf->mutable_data() + remaining, buf->capacity() - remaining);
        // Set receive time point.
        buf->SetRecvTime(Timer::now());

        // Get some stats from recv() return value.
        auto bytes_received = std::get<0>(recv_status);
        auto sock_status = std::get<1>(recv_status).get_value();
        this->bytes_received_ += bytes_received;

        // Scan the buffer for JSONs.
        auto scan_size = remaining + bytes_received;
        auto scan = buf->Scan(scan_size, this->seq);

        // Increase current sequence number.
        this->seq += scan.first;
        // Increase number of received JSONs.
        this->jsons_received_ += scan.first;

        // Deal with the remaining bytes.
        remaining = scan.second;
        ILLEX_ROE(buf->SetSize(scan_size - remaining));
        // Copy leftover bytes to temporary buffer.
        if (remaining > 0) {
          std::memcpy(spill, buf->data() + buf->size(), remaining);
        }

        // Perhaps the server disconnected because it's done sending JSONs, check the
        // status.
        if (sock_status == kissnet::socket_status::cleanly_disconnected) {
          SPDLOG_DEBUG("Server has cleanly disconnected.");
          mutexes[lock_idx]->unlock();
          free(spill);
          return Status::OK();
        } else if (sock_status != kissnet::socket_status::valid) {
          // Otherwise, if it's not valid, there is something wrong.
          return Status(Error::ClientError,
                        "Server error. Status: " + std::to_string(sock_status));
        }

        // Unlock the buffer.
        mutexes[lock_idx]->unlock();
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    } catch (const std::exception& e) {
      // But first we catch any exceptions.
      return Status(Error::ClientError, e.what());
    }
  }

  free(spill);
  return Status::OK();
}

auto BufferingClient::Close() -> Status {
  if (must_be_closed) {
    client->close();
    must_be_closed = false;
  } else {
    return Status(Error::ClientError, "Client was already closed.");
  }
  return Status::OK();
}

auto BufferingClient::bytes_received() const -> size_t { return bytes_received_; }

auto BufferingClient::jsons_received() const -> size_t { return jsons_received_; }

BufferingClient::~BufferingClient() { Close(); }

}  // namespace illex