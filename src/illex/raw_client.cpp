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

#include <chrono>
#include <thread>
#include <memory>
#include <iostream>

#include "illex/raw_client.h"

namespace illex {

using TCPBuffer = std::array<std::byte, illex::RAW_BUFFER_SIZE>;

auto RawClient::Create(RawProtocol protocol, std::string host, uint64_t seq, RawClient *out) -> Status {
  out->seq = seq;
  out->host = std::move(host);
  out->protocol = protocol;

  auto endpoint = out->host + ":" + std::to_string(out->protocol.port);
  out->client = std::make_shared<RawSocket>(kissnet::endpoint(endpoint));

  SPDLOG_DEBUG("Client connecting to {}...", endpoint);
  out->client->connect();

  return Status::OK();
}

/**
 * \brief Enqueue all JSONs in the TCP buffer.
 * \param[out]      json_buffer     Reusable JSON string buffer.
 * \param[in,out]   tcp_buffer      The TCP buffer that is cleared after all JSONs are queued.
 * \param[in]       tcp_valid_bytes Number of valid bytes in the TCP buffer.
 * \param[out]      queue           The queue to enqueue the JSON queue items in.
 * \param[in,out]   seq             The sequence number for the next JSON item, is increased when item is enqueued.
 * \return The number of JSONs enqueued.
 */
static auto EnqueueAllJSONsInBuffer(std::string *json_buffer,
                                    TCPBuffer *tcp_buffer,
                                    size_t tcp_valid_bytes,
                                    JSONQueue *queue,
                                    uint64_t *seq) -> size_t {
  size_t queued = 0;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.
  auto *recv_chars = reinterpret_cast<char *>(tcp_buffer->data());

  // Scan the buffer for a newline.
  char *json_start = recv_chars;
  char *json_end = recv_chars + tcp_valid_bytes;
  size_t remaining = tcp_valid_bytes;
  do {
    json_end = std::strchr(json_start, '\n');
    if (json_end == nullptr) {
      // Append the remaining characters to the JSON buffer.
      json_buffer->append(json_start, remaining);
      // We appended everything up to the end of the buffer, so we can set remaining bytes to 0.
      remaining = 0;
    } else {
      // There is a newline. Only append the remaining characters to the json_msg.
      json_buffer->append(json_start, json_end - json_start);

      // Copy the JSON string into the consumption queue.
      SPDLOG_DEBUG("Client received JSON[{}]: {}", *seq, *json_buffer);
      queue->enqueue(JSONQueueItem{*seq, *json_buffer});
      (*seq)++;
      queued++;

      // Clear the JSON string buffer.
      // The implementation of std::string is allowed to change its allocated buffer here, but there
      // are no implementations that actually do it, they retain the allocated buffer.
      // An implementation using std::vector<char> might be desired here just to make sure.
      json_buffer->clear();

      // Move the start to the character after the newline.
      json_start = json_end + 1;

      // Calculate the remaining number of bytes in the buffer.
      remaining = (recv_chars + tcp_valid_bytes) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while (remaining > 0);

  // Clear the buffer when finished.
  tcp_buffer->fill(std::byte(0x00));

  return queued;
}

auto RawClient::ReceiveJSONs(JSONQueue *queue, putong::Timer<> *latency_timer) -> Status {
  bool first = true;

  // Buffer to store the JSON string, is reused to prevent allocations.
  std::string json_string;
  // TCP receive buffer.
  TCPBuffer recv_buffer{};

  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to receive some bytes.
      auto recv_status = client->recv(recv_buffer);

      // Start the latency timer.
      if (first && latency_timer != nullptr) {
        latency_timer->Start();
        first = false;
      }

      // Perhaps the server disconnected because it's done sending JSONs, check the status.
      auto bytes_received = std::get<0>(recv_status);
      auto sock_status = std::get<1>(recv_status).get_value();
      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::RawError, "Server error. Status: " + std::to_string(sock_status));
      }
      this->bytes_received_ += bytes_received;
      // We must now handle the received bytes in the TCP buffer.
      this->received_ += EnqueueAllJSONsInBuffer(&json_string, &recv_buffer, bytes_received, queue, &this->seq);
    } catch (const std::exception &e) {
      // But first we catch any exceptions.
      return Status(Error::RawError, e.what());
    }
  }

  return Status::OK();
}

auto RawClient::Close() -> Status {
  SPDLOG_DEBUG("Closing client.");
  client->close();
  return Status::OK();
}

}
