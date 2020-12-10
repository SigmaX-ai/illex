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

auto RawJSONBuffer::SetSequenceNumbers(std::pair<Seq, Seq> seq_nos) -> Status {
  seq_first_last = seq_nos;
  return Status::OK();
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
static auto CountJSONsInBuffer(RawJSONBuffer *buffer,
                               uint64_t *seq,
                               TimePoint receive_time,
                               LatencyTracker *tracker = nullptr)
-> std::pair<Seq, Seq> {
  size_t first = *seq;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.

  // Scan the buffer for a newline.
  const auto *recv_chars = reinterpret_cast<const char *>(buffer->data());
  const char *json_start = recv_chars;
  const char *json_end = recv_chars + buffer->size();
  size_t remaining = buffer->size();
  do {
    json_end = std::strchr(json_start, '\n');
    if (json_end == nullptr) {
      remaining = 0;
    } else {
      // Place the receive time for this JSON in the tracker.
      if (tracker != nullptr) { tracker->Put(*seq, 0, receive_time); }
      (*seq)++;
      // Move the start scan index to the character after the newline.
      json_start = json_end + 1;
      // Calculate the remaining number of bytes in the buffer.
      remaining = (recv_chars + buffer->size()) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while (remaining > 0);

  return {first, (*seq) - 1};
}

auto DirectBufferClient::ReceiveJSONs(LatencyTracker *lat_tracker) -> Status {
  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      bool empty = true;
      RawJSONBuffer *buf = nullptr;
      // Find an empty buffer mutex to lock.
      while (!empty || !mutexes[buffer_idx]->try_lock()) {
        buf = buffers[buffer_idx];
        // Check if the buffer still has some data for a downstream thread to process.
        if (!buf->empty()) {
          mutexes[buffer_idx]->unlock();
          empty = false;
        }
        // Increment and wrap around the current buffer index.
        buffer_idx = (buffer_idx + 1) % buffers.size();
      }
      // We were able to lock a buffer mutex.

      // Attempt to receive some bytes.
      auto recv_status = client->recv(buf->mutable_data(), buf->size());
      auto receive_time = Timer::now();

      // Perhaps the server disconnected because it's done sending JSONs, check the
      // status.
      auto bytes_received = std::get<0>(recv_status);
      auto sock_status = std::get<1>(recv_status).get_value();
      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::RawError,
                      "Server error. Status: " + std::to_string(sock_status));
      }
      this->total_bytes_received_ += bytes_received;
      ILLEX_ROE(buf->SetSize(bytes_received));
      // We must now handle the received bytes in the TCP buffer.
      auto num_enqueued = CountJSONsInBuffer(buf, &this->seq, receive_time, lat_tracker);
      ILLEX_ROE(buf->SetSequenceNumbers(num_enqueued))
      this->jsons_received_ += (num_enqueued.second - num_enqueued.first + 1);
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