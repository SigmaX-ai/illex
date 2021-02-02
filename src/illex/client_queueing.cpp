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

#include "illex/client_queueing.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>

#include "illex/latency.h"
#include "illex/protocol.h"

namespace illex {

auto QueueingClient::Close() -> Status {
  if (must_be_closed) {
    client->close();
    must_be_closed = false;
  } else {
    return Status(Error::ClientError, "Client was already closed.");
  }
  return Status::OK();
}

QueueingClient::~QueueingClient() {
  if (must_be_closed) {
    this->Close();
  }
  free(buffer);
}

auto QueueingClient::Create(const ClientOptions& options, JSONQueue* queue,
                            QueueingClient* out, size_t buffer_size) -> Status {
  assert(out != nullptr);
  assert(queue != nullptr);

  out->seq = options.seq;
  out->buffer = static_cast<std::byte*>(malloc(buffer_size));
  if (out->buffer == nullptr) {
    return Status(Error::ClientError, "Could not allocate TCP recv buffer.");
  }
  out->buffer_size = buffer_size;
  out->queue = queue;

  auto endpoint = options.host + ":" + std::to_string(options.port);
  out->client = std::make_shared<Socket>(kissnet::endpoint(endpoint));

  SPDLOG_DEBUG("Client connecting to {}...", endpoint);
  auto success = out->client->connect();
  if (!success) {
    return Status(Error::ClientError, "Unable to connect to server.");
  }

  out->must_be_closed = true;

  return Status::OK();
}

/**
 * \brief Enqueue all JSONs in the TCP buffer.
 * \param[out]      json_buffer     Reusable JSON string buffer.
 * \param[in,out]   recv_buffer     The TCP buffer that is cleared after all JSONs are
 *                                  queued.
 * \param[in]       tcp_valid_bytes Number of valid bytes in the TCP buffer.
 * \param[out]      queue           The queue to enqueue the JSON queue items in.
 * \param[in,out]   seq             The sequence number for the next JSON item, is
 *                                  increased when item is enqueued.
 * \param[in]       receive_time    Point in time when this buffer was received.
 * \param[out]      tracker         Latency tracking device, set to nullptr if unused.
 * \return The number of JSONs enqueued.
 */
static auto EnqueueAllJSONsInBuffer(std::string* json_buffer, std::byte* recv_buffer,
                                    size_t tcp_valid_bytes, JSONQueue* queue,
                                    uint64_t* seq, TimePoint receive_time,
                                    LatencyTracker* tracker = nullptr) -> size_t {
  size_t queued = 0;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.
  auto* recv_chars = reinterpret_cast<char*>(recv_buffer);

  // Scan the buffer for a newline.
  char* json_start = recv_chars;
  char* json_end = recv_chars + tcp_valid_bytes;
  size_t remaining = tcp_valid_bytes;
  do {
    json_end = static_cast<char*>(std::memchr(json_start, '\n', remaining));
    if (json_end == nullptr) {
      // Append the remaining characters to the JSON buffer.
      json_buffer->append(json_start, remaining);
      // We appended everything up to the end of the buffer, so we can set remaining
      // bytes to 0.
      remaining = 0;
    } else {
      // There is a newline. Only append the remaining characters to the json_msg.
      json_buffer->append(json_start, json_end - json_start);

      // Copy the JSON string into the consumption queue.
      auto pre_queue_time = Timer::now();
      queue->enqueue(JSONItem{*seq, *json_buffer});
      // Place the receive time for this JSON in the tracker.
      if (tracker != nullptr) {
        tracker->Put(*seq, 0, receive_time);
        tracker->Put(*seq, 1, pre_queue_time);
      }
      (*seq)++;
      queued++;

      // Clear the JSON string buffer. The implementation of std::string is allowed to
      // change its allocated buffer here, but there are no implementations that actually
      // do it, they retain the allocated buffer. An implementation using
      // std::vector<char> might be desired here just to make sure.
      json_buffer->clear();

      // Move the start to the character after the newline.
      json_start = json_end + 1;

      // Calculate the remaining number of bytes in the buffer.
      remaining = (recv_chars + tcp_valid_bytes) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while (remaining > 0);

  // Clear the buffer when finished.
  memset(recv_buffer, 0, ILLEX_DEFAULT_TCP_BUFSIZE);

  return queued;
}

auto QueueingClient::ReceiveJSONs(LatencyTracker* lat_tracker) -> Status {
  // Buffer to store the JSON string, is reused to prevent allocations.
  std::string json_string;

  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to receive some bytes.
      auto recv_status = client->recv(buffer, buffer_size);
      auto receive_time = Timer::now();

      // Perhaps the server disconnected because it's done sending JSONs, check the
      // status.
      auto bytes_received = std::get<0>(recv_status);
      auto sock_status = std::get<1>(recv_status).get_value();

      this->bytes_received_ += bytes_received;
      // We must now handle the received bytes in the TCP buffer.
      auto num_enqueued =
          EnqueueAllJSONsInBuffer(&json_string, buffer, bytes_received, queue, &this->seq,
                                  receive_time, lat_tracker);
      this->received_ += num_enqueued;

      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::ClientError,
                      "Server error. Status: " + std::to_string(sock_status));
      }
    } catch (const std::exception& e) {
      // But first we catch any exceptions.
      return Status(Error::ClientError, e.what());
    }
  }

  return Status::OK();
}

}  // namespace illex
