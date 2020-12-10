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

#pragma once

#include <string>
#include <utility>
#include <mutex>
#include <memory>

#include <kissnet.hpp>
#include <putong/timer.h>

#include "illex/log.h"
#include "illex/status.h"
#include "illex/queue.h"
#include "illex/raw_protocol.h"
#include "illex/latency.h"

namespace illex {

struct RawClient {
  virtual auto ReceiveJSONs(LatencyTracker *lat_tracker) -> Status = 0;
  virtual auto Close() -> Status = 0;
};

/// A streaming client using the Raw protocol that queues received JSONs.
struct RawQueueingClient : public RawClient {
 public:
  /**
   * \brief Construct a new queueing client.
   * \param[in] protocol    The protocol options.
   * \param[in] host        The hostname to connect to.
   * \param[in] seq         Starting sequence number.
   * \param[in] queue           The queue to put the JSONs in.
   * \param[out] out        The raw client that will be populated by this function.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(RawProtocol protocol,
                     std::string host,
                     uint64_t seq,
                     JSONQueue *queue,
                     RawQueueingClient *out,
                     size_t buffer_size = ILLEX_TCP_BUFFER_SIZE) -> Status;

  /**
   * \brief Receive JSONs on this raw stream client and put them in a queue.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(LatencyTracker *lat_tracker) -> Status override;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Close() -> Status override;

  /// \brief Return the number of received JSONs
  [[nodiscard]] auto received() const -> size_t { return received_; }

  /// \brief Return the number of received bytes
  [[nodiscard]] auto bytes_received() const -> size_t { return bytes_received_; }

  RawQueueingClient() = delete;
  ~RawQueueingClient();
 private:
  /// The next available sequence number.
  uint64_t seq = 0;
  /// The number of received JSONs.
  size_t received_ = 0;
  /// The number of received bytes.
  size_t bytes_received_ = 0;
  /// The host name to connect to.
  std::string host = "localhost";
  /// The protocol options.
  illex::RawProtocol protocol;
  /// The TCP socket.
  std::shared_ptr<RawSocket> client;
  // TCP receive buffer.
  std::byte *buffer;
  // TCP receive buffer size.
  size_t buffer_size;
  // Whether the client must be closed.
  bool must_be_closed = false;
  // The queue to dump JSONs in.
  JSONQueue *queue;
};

/// A structure to manage multi-buffered client implementation.
class RawJSONBuffer {
 public:
  /**
   * \brief Create a new buffer wrapper to dump JSONs in.
   * \param[in]  buffer      The buffer address, managed elsewhere.
   * \param[in]  capacity    The capacity of the buffer.
   * \param[out] out         The resulting
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(std::byte *buffer, size_t capacity, RawJSONBuffer *out) -> Status;

  /// \brief Return a pointer to mutate the buffer contents.
  auto mutable_data() -> std::byte * { return buffer_; }

  /// \brief Return a pointer to read the buffer.
  [[nodiscard]] auto data() const -> const std::byte * { return buffer_; }

  /// \brief Return the allocated capacity of the buffer.
  [[nodiscard]] auto capacity() const -> size_t { return capacity_; }

  /// \brief Return the number of valid bytes in the buffer.
  [[nodiscard]] auto size() const -> size_t { return size_; }

  /// \brief Return whether the buffer is empty.
  [[nodiscard]] auto empty() const -> bool { return size_ == 0; }

  /// \brief Modify the number of valid of bytes in the buffer without bounds checking.
  inline void SetSizeUnsafe(size_t size) { size_ = size; }

  /// \brief Modify the number of valid bytes in the buffer with bounds checking.
  auto SetSize(size_t size) -> Status;

  /// \brief Set the JSON sequence number inclusive range contained in this buffer.
  auto SetSequenceNumbers(std::pair<Seq, Seq> seq_nos) -> Status;

  /// \brief Reset the buffer.
  void Reset() { this->size_ = 0; }
 protected:
  RawJSONBuffer(std::byte *buffer, size_t capacity)
      : buffer_(buffer), capacity_(capacity) {}
  /// A pointer to the buffer.
  std::byte *buffer_;
  /// The number of valid bytes in the buffer.
  size_t size_ = 0;
  /// The capacity of the buffer.
  size_t capacity_ = 0;
  /// The JSONs sequence numbers contained within the buffer.
  std::pair<Seq, Seq> seq_first_last;
};

/// A streaming client using the Raw protocol that receives the JSONs over multiple
/// buffers with mutexes. It applies a non-blocking round-robin approach to finding an
/// available buffer to dump the contents in, but keeps track of sequence numbers to not
/// lose order.
struct RawMultiBufferClient : public RawClient {
 public:
  /**
   * \brief Construct a RawMultiBufferClient
   * \param[in]  protocol The protocol options.
   * \param[in]  host The hostname.
   * \param[in]  seq The sequence number to start at.
   * \param[in]  buffers The pre-allocated buffers.
   * \param[out] out The RawMultiBufferClient output.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(RawProtocol protocol,
                     std::string host,
                     uint64_t seq,
                     const std::vector<RawJSONBuffer *> &buffers,
                     const std::vector<std::mutex *> &mutexes,
                     RawMultiBufferClient *out) -> Status;

  /**
   * \brief Receive JSONs into the pre-allocated buffers.
   *
   * This function is meant to run in a main thread, where consuming threads access the
   * buffers through buffer() and mutex() to absorb the data. Consuming threads must
   * Reset() the buffers after they are done processing the data.
   *
   * \param lat_tracker Latency tracking device.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(LatencyTracker *lat_tracker) -> Status override;
  auto Close() -> Status override;
 private:
  /// The mutexes to manage buffer access.
  std::vector<std::mutex *> mutexes;
  /// The buffers.
  std::vector<RawJSONBuffer *> buffers;
  /// The next available sequence number.
  uint64_t seq = 0;
  /// The number of received JSONs.
  size_t jsons_received_ = 0;
  /// The number of received bytes.
  size_t total_bytes_received_ = 0;
  /// The host name to connect to.
  std::string host = "localhost";
  /// The protocol options.
  illex::RawProtocol protocol;
  /// The TCP socket.
  std::shared_ptr<RawSocket> client;
  // Whether the client must be closed.
  bool must_be_closed = false;
  // The current buffer to receive the TCP data in.
  size_t buffer_idx = 0;
};

}
