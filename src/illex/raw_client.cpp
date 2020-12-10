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
#include <memory>
#include <iostream>
#include <utility>

#include "illex/latency.h"
#include "illex/raw_client.h"
#include "illex/raw_protocol.h"

namespace illex {

auto RawQueueingClient::Create(RawProtocol protocol,
                               std::string host,
                               uint64_t seq,
                               JSONQueue *queue,
                               RawQueueingClient *out,
                               size_t buffer_size) -> Status {
  auto *buffer = static_cast<std::byte *>(malloc(buffer_size));

  if (buffer == nullptr) {
    return Status(Error::RawError, "Could not allocate TCP recv buffer.");
  }

  out->seq = seq;
  out->host = std::move(host);
  out->protocol = protocol;
  out->buffer = buffer;
  out->buffer_size = buffer_size;
  out->queue = queue;

  auto endpoint = out->host + ":" + std::to_string(out->protocol.port);
  out->client = std::make_shared<RawSocket>(kissnet::endpoint(endpoint));

  SPDLOG_DEBUG("Client connecting to {}...", endpoint);
  auto success = out->client->connect();
  if (!success) {
    return Status(Error::RawError, "Unable to connect to server.");
  }

  out->must_be_closed = true;

  return Status::OK();
}

/**
 * \brief Enqueue all JSONs in the TCP buffer.
 * \param[out]      json_buffer     Reusable JSON string buffer.
 * \param[in,out]   tcp_buffer      The TCP buffer that is cleared after all JSONs are
 *                                   queued.
 * \param[in]       tcp_valid_bytes Number of valid bytes in the TCP buffer.
 * \param[out]      queue           The queue to enqueue the JSON queue items in.
 * \param[in,out]   seq             The sequence number for the next JSON item, is
 *                                  increased when item is enqueued.
 * \return The number of JSONs enqueued.
 */
static auto EnqueueAllJSONsInBuffer(std::string *json_buffer,
                                    std::byte *recv_buffer,
                                    size_t tcp_valid_bytes,
                                    JSONQueue *queue,
                                    uint64_t *seq,
                                    TimePoint receive_time,
                                    LatencyTracker *tracker = nullptr)
-> size_t {
  size_t queued = 0;
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.
  auto *recv_chars = reinterpret_cast<char *>(recv_buffer);

  // Scan the buffer for a newline.
  char *json_start = recv_chars;
  char *json_end = recv_chars + tcp_valid_bytes;
  size_t remaining = tcp_valid_bytes;
  do {
    json_end = std::strchr(json_start, '\n');
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
      queue->enqueue(JSONQueueItem{*seq, *json_buffer});
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
  memset(recv_buffer, 0, ILLEX_TCP_BUFFER_SIZE);

  return queued;
}

auto RawQueueingClient::ReceiveJSONs(LatencyTracker *lat_tracker) -> Status {
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
      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::RawError,
                      "Server error. Status: " + std::to_string(sock_status));
      }
      this->bytes_received_ += bytes_received;
      // We must now handle the received bytes in the TCP buffer.
      auto num_enqueued = EnqueueAllJSONsInBuffer(&json_string,
                                                  buffer,
                                                  bytes_received,
                                                  queue,
                                                  &this->seq,
                                                  receive_time,
                                                  lat_tracker);
      this->received_ += num_enqueued;
    } catch (const std::exception &e) {
      // But first we catch any exceptions.
      return Status(Error::RawError, e.what());
    }
  }

  return Status::OK();
}

auto RawQueueingClient::Close() -> Status {
  if (must_be_closed) {
    client->close();
    must_be_closed = false;
  } else {
    return Status(Error::RawError, "Client was already closed.");
  }
  return Status::OK();
}

RawQueueingClient::~RawQueueingClient() {
  if (must_be_closed) {
    this->Close();
  }
  free(buffer);
}

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

Status RawJSONBuffer::SetSize(size_t size) {
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

auto RawMultiBufferClient::Create(RawProtocol protocol,
                                  std::string host,
                                  uint64_t seq,
                                  const std::vector<RawJSONBuffer *> &buffers,
                                  const std::vector<std::mutex *> &mutexes,
                                  RawMultiBufferClient *out) -> Status {
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

auto RawMultiBufferClient::ReceiveJSONs(LatencyTracker *lat_tracker) -> Status {
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
        buffer_idx++;
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

auto RawMultiBufferClient::Close() -> Status {
  if (must_be_closed) {
    client->close();
    must_be_closed = false;
  } else {
    return Status(Error::RawError, "Client was already closed.");
  }
  return Status::OK();
}

}
