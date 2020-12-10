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

#include "illex/latency.h"
#include "illex/status.h"
#include "illex/client_buffered.h"
#include "illex/log.h"

namespace illex {

auto RawJSONBuffer::Create(std::byte *buffer,
                           size_t capacity,
                           RawJSONBuffer *out) -> Status {
  if (buffer == nullptr) {
    return Status(Error::RawError, "Pre-allocated buffer cannot be nullptr.");
  }
  if (capacity == 0) {
    return Status(Error::RawError, "Size cannot be 0.");
  }
  *out = RawJSONBuffer(buffer, capacity);

  return Status::OK();
}

auto RawJSONBuffer::SetSize(size_t size) -> Status {
  if (size < capacity_) {
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
auto RawJSONBuffer::CountJSONs(uint64_t *seq,
                               TimePoint receive_time,
                               LatencyTracker *tracker)
-> size_t {
  size_t first = *seq;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.

  // Scan the buffer for a newline.
  const auto *recv_chars = reinterpret_cast<const char *>(this->data());
  const char *json_start = recv_chars;
  const char *json_end = recv_chars + this->size();
  size_t remaining = this->size();
  do {
    json_end = static_cast<const char *>(std::memchr(json_start, '\n', remaining));
    if (json_end == nullptr) {
      remaining = 0;
    } else {
      // Place the receive time for this JSON in the tracker.
      if (tracker != nullptr) { tracker->Put(*seq, 0, receive_time); }
      (*seq)++;
      // Move the start scan index to the character after the newline.
      json_start = json_end + 1;
      // Calculate the remaining number of bytes in the buffer.
      remaining = (recv_chars + this->size()) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while (remaining > 0);

  // Set contained sequence numbers.
  this->seq_first_last = {first, (*seq) - 1};

  return (*seq) - first;
}

auto DirectBufferClient::ReceiveJSONs(LatencyTracker *lat_tracker) -> Status {
  const size_t num_buffers = this->buffers.size();
  size_t next_buf_idx = 0;
  size_t current_buf_idx = 0;
  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to get a lock on an empty buffer.
      illex::RawJSONBuffer *buf = nullptr;
      do {
        current_buf_idx = next_buf_idx;
        auto lock = mutexes[current_buf_idx]->try_lock();
        if (lock) {
          if (!buffers[current_buf_idx]->empty()) {
            mutexes[current_buf_idx]->unlock();
          } else {
            buf = buffers[current_buf_idx];
          }
        }
        next_buf_idx = (current_buf_idx + 1) % num_buffers;
      } while (buf == nullptr);
      // We now have an empty buffer.

      // Attempt to receive some bytes.
      auto recv_status = client->recv(buf->mutable_data(), buf->capacity());
      auto receive_time = Timer::now();

      // Perhaps the server disconnected because it's done sending JSONs, check the
      // status.
      auto bytes_received = std::get<0>(recv_status);
      auto sock_status = std::get<1>(recv_status).get_value();

      this->total_bytes_received_ += bytes_received;
      ILLEX_ROE(buf->SetSize(bytes_received));
      auto received = buf->CountJSONs(&this->seq, receive_time, lat_tracker);
      this->jsons_received_ += received;

      // The buffer is ready for processing. Unlock it.
      mutexes[current_buf_idx]->unlock();

      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::RawError,
                      "Server error. Status: " + std::to_string(sock_status));
      }
    } catch (const std::exception &e) {
      // But first we catch any exceptions.
      return Status(Error::RawError, e.what());
    }
  }

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