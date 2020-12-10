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

#include <cstddef>
#include <vector>
#include <mutex>
#include <chrono>
#include <thread>

#include "illex/latency.h"
#include "illex/status.h"
#include "illex/client_buffered.h"
#include "illex/log.h"

namespace illex {

auto RawJSONBuffer::Create(std::byte *buffer,
                           size_t capacity,
                           RawJSONBuffer *out,
                           size_t lat_track_cap) -> Status {
  if (buffer == nullptr) {
    return Status(Error::RawError, "Pre-allocated buffer cannot be nullptr.");
  }
  if (capacity == 0) {
    return Status(Error::RawError, "Size cannot be 0.");
  }
  *out = RawJSONBuffer(buffer, capacity);
  out->seq_tracked_.reserve(lat_track_cap);

  return Status::OK();
}

auto RawJSONBuffer::SetSize(size_t size) -> Status {
  if (size <= capacity_) {
    size_ = size;
    return Status::OK();
  } else {
    return Status(Error::RawError,
                  "Cannot set buffer size larger than allocated capacity.");
  }
}

auto DirectBufferClient::Create(RawProtocol protocol,
                                std::string host,
                                uint64_t seq,
                                const std::vector<RawJSONBuffer *> &buffers,
                                const std::vector<std::mutex *> &mutexes,
                                DirectBufferClient *out) -> Status {
  // Sanity check.
  // TODO: we could also get a vector of a buffer-mutex pair
  if (mutexes.size() != buffers.size()) {
    return Status(Error::RawError,
                  "Cannot create client. "
                  "Number of buffers mismatches number of mutexes.");
  }
  out->mutexes = mutexes;
  out->buffers = buffers;
  // Set other configuration params.
  out->seq = seq;
  out->host = std::move(host);
  out->protocol = protocol;
  // Create an endpoint.
  auto endpoint = out->host + ":" + std::to_string(out->protocol.port);
  out->client = std::make_shared<RawSocket>(kissnet::endpoint(endpoint));
  // Attempt to connect.
  SPDLOG_DEBUG("Client connecting to {}...", endpoint);
  auto success = out->client->connect();
  if (!success) {
    return Status(Error::RawError, "Unable to connect to server.");
  }
  // Connect successful, remember we have to close it on deconstruction.
  out->must_be_closed = true;

  return Status::OK();
}

/**
 * \brief Count the JSONs in the buffer, so we can assign sequence numbers.
 * \param buffer
 * \param tcp_valid_bytes
 * \param seq
 * \param receive_time
 * \param tracker
 * \return
 */
auto RawJSONBuffer::Scan(size_t num_bytes,
                         uint64_t *seq,
                         TimePoint receive_time,
                         LatencyTracker *tracker)
-> std::pair<size_t, size_t> {
  size_t first = *seq;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.

  // Scan the buffer for a newline.
  const auto *recv_chars = reinterpret_cast<const char *>(this->data());
  const char *json_start = recv_chars;
  const char *json_end = recv_chars + this->size();
  size_t remaining = num_bytes;
  do {
    json_end = static_cast<const char *>(std::memchr(json_start, '\n', remaining));
    if (json_end != nullptr) {
      auto json_len = json_end - json_start;
      if (json_len > 0) {
        // Place the receive time for this JSON in the tracker.
        if (tracker != nullptr) {
          if (tracker->Put(*seq, 0, receive_time)) {
            seq_tracked_.push_back(*seq);
          }
        }
        (*seq)++;
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
  this->seq_first_last = {first, (*seq) - 1};

  // Return number of JSONs and number of remaining bytes.
  return {(*seq) - first, remaining};
}

void RawJSONBuffer::Reset() {
  size_ = 0;
  seq_first_last = {0, 0};
  seq_tracked_.clear();
}

auto TryGetEmptyBuffer(const std::vector<RawJSONBuffer *> &buffers,
                       const std::vector<std::mutex *> &mutexes,
                       RawJSONBuffer **out,
                       size_t *lock_idx) -> bool {
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

auto DirectBufferClient::ReceiveJSONs(LatencyTracker *lat_tracker) -> Status {
  // Buffer to temporary place leftover bytes from a buffer.
  auto *spill = static_cast<std::byte *>(std::malloc(ILLEX_TCP_BUFFER_SIZE));
  // Bytes leftover from previous buffer.
  size_t remaining = 0;

  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to get a lock on an empty buffer.
      RawJSONBuffer *buf;
      size_t lock_idx;
      if (TryGetEmptyBuffer(buffers, mutexes, &buf, &lock_idx)) {
        // Copy leftovers from previous buffer into new buffer.
        if (remaining > 0) {
          std::memcpy(buf->mutable_data(), spill, remaining);
        }

        // Attempt to receive some bytes.
        auto recv_status = client->recv(buf->mutable_data() + remaining,
                                        buf->capacity() - remaining);
        auto receive_time = Timer::now();

        auto bytes_received = std::get<0>(recv_status);
        auto sock_status = std::get<1>(recv_status).get_value();
        auto scan_size = remaining + bytes_received;

        this->total_bytes_received_ += bytes_received;
        auto scan = buf->Scan(scan_size, &this->seq, receive_time, lat_tracker);
        this->jsons_received_ += scan.first;
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
          return Status(Error::RawError,
                        "Server error. Status: " + std::to_string(sock_status));
        }
        mutexes[lock_idx]->unlock();
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    } catch (const std::exception &e) {
      // But first we catch any exceptions.
      return Status(Error::RawError, e.what());
    }
  }

  free(spill);
  return Status::OK();
}

auto DirectBufferClient::Close() -> Status {
  if (must_be_closed) {
    client->close();
    must_be_closed = false;
  } else {
    return Status(Error::RawError, "Client was already closed.");
  }
  return Status::OK();
}

}