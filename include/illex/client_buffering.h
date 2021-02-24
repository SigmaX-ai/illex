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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "illex/client.h"
#include "illex/protocol.h"

#pragma once

namespace illex {

/// Range of sequence numbers.
struct SeqRange {
  /// The first sequence number in the range.
  Seq first;
  /// The last sequence number in the range.
  Seq last;
};

/// A structure to manage multi-buffered client implementation.
class JSONBuffer {
 public:
  /**
   * \brief Create a new buffer wrapper to dump JSONs in.
   * \param[in]  buffer         The buffer address, managed elsewhere.
   * \param[in]  capacity       The capacity of the buffer.
   * \param[out] out            The resulting
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(std::byte* buffer, size_t capacity, JSONBuffer* out) -> Status;

  /**
   * \brief Scan the first num_bytes bytes in the buffer for newline delimited JSONs.
   *
   * This function returns the number of newline delimited JSONs.
   *
   * \param num_bytes Number of bytes to scan from the start.
   * \param seq       THe starting sequence number for the JSONs in this buffer.
   * \return          A pair containing the number of JSONs and the remaining bytes.
   */
  auto Scan(size_t num_bytes, uint64_t seq) -> std::pair<size_t, size_t>;

  /// \brief Return a pointer to mutate the buffer contents.
  auto mutable_data() -> std::byte* { return buffer_; }

  /// \brief Return a pointer to read the buffer.
  [[nodiscard]] inline auto data() const -> const std::byte* { return buffer_; }

  /// \brief Return the allocated capacity of the buffer.
  [[nodiscard]] inline auto capacity() const -> size_t { return capacity_; }

  /// \brief Return the number of valid bytes in the buffer.
  [[nodiscard]] inline auto size() const -> size_t { return size_; }

  /// \brief Return whether the buffer is empty.
  [[nodiscard]] inline auto empty() const -> bool { return size_ == 0; }

  void SetRange(SeqRange range) { seq_range = range; }
  [[nodiscard]] inline auto range() const -> SeqRange { return seq_range; }
  [[nodiscard]] auto num_jsons() const -> size_t;

  /// \brief Modify the number of valid of bytes in the buffer without bounds checking.
  inline void SetSizeUnsafe(size_t size) { size_ = size; }

  /// \brief Modify the number of valid bytes in the buffer with bounds checking.
  auto SetSize(size_t size) -> Status;

  /// \brief Set the receive time of this buffer.
  void SetRecvTime(TimePoint time) { this->recv_time_ = time; }

  /// \brief Get the receive time of this buffer.
  auto recv_time() -> TimePoint { return this->recv_time_; }

  /// \brief Reset the buffer.
  void Reset();

  JSONBuffer() = default;

 protected:
  JSONBuffer(std::byte* buffer, size_t capacity) : buffer_(buffer), capacity_(capacity) {}
  /// A pointer to the buffer.
  std::byte* buffer_ = nullptr;
  /// The number of valid bytes in the buffer.
  size_t size_ = 0;
  /// The capacity of the buffer.
  size_t capacity_ = 0;
  /// The JSONs sequence numbers contained within the buffer.
  SeqRange seq_range = {0, 0};
  /// The TCP receive time point of this buffer.
  TimePoint recv_time_;
};

/**
 * \brief A client that buffers received JSONs.
 *
 * This client can be supplied to work with multiple, lockable buffers. When the client
 * has obtained a buffer lock, it will attempt to fill the buffer until it is full, or
 * until there are no TCP packets to deliver. It will then unlock the buffer. This allows
 * multiple downstream threads to consume from multiple buffers simultaneously.
 *
 * The client keeps track of the order of received JSONs by adding sequence numbers.
 */
class BufferingClient : public Client {
 public:
  ~BufferingClient();

  /**
   * \brief Create a new buffering client.
   *
   * \param options The options for this client.
   * \param buffers A set of pre-allocated buffers on which to operate.
   * \param mutexes A set of mutexes to obtain locks on each buffer.
   * \param out     The BufferingClient object to populate.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(const ClientOptions& options,
                     const std::vector<JSONBuffer*>& buffers,
                     const std::vector<std::mutex*>& mutexes, BufferingClient* out)
      -> Status;

  auto ReceiveJSONs(LatencyTracker* lat_tracker = nullptr) -> Status override;
  auto Close() -> Status override;
  [[nodiscard]] auto jsons_received() const -> size_t override;
  [[nodiscard]] auto bytes_received() const -> size_t override;

 private:
  /// The mutexes to manage buffer access.
  std::vector<std::mutex*> mutexes;
  /// The buffers.
  std::vector<JSONBuffer*> buffers;
  // Whether the client must be closed.
  bool must_be_closed = false;
  // The current buffer to receive the TCP data in.
  size_t buffer_idx = 0;
  /// The next available sequence number.
  Seq seq = 0;
  /// The number of received JSONs.
  size_t jsons_received_ = 0;
  /// The number of received bytes.
  size_t bytes_received_ = 0;
  /// The TCP socket.
  std::shared_ptr<Socket> client;
};

}  // namespace illex
