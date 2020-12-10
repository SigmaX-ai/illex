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

#include <vector>
#include <mutex>
#include <memory>
#include <string>

#include "illex/protocol.h"
#include "illex/client.h"

#pragma once

namespace illex {

/// A structure to manage multi-buffered client implementation.
class RawJSONBuffer {
 public:
  /**
   * \brief Create a new buffer wrapper to dump JSONs in.
   * \param[in]  buffer         The buffer address, managed elsewhere.
   * \param[in]  capacity       The capacity of the buffer.
   * \param[out] out            The resulting
   * \param[in]  lat_track_cap  Initial capacity for latency tracking seq no buffer.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(std::byte *buffer,
                     size_t capacity,
                     RawJSONBuffer *out,
                     size_t lat_track_cap = 8) -> Status;

  auto Scan(size_t num_bytes,
            uint64_t *seq,
            TimePoint receive_time,
            LatencyTracker *tracker = nullptr) -> std::pair<size_t, size_t>;

  /// \brief Return a pointer to mutate the buffer contents.
  auto mutable_data() -> std::byte * { return buffer_; }

  /// \brief Return a pointer to read the buffer.
  [[nodiscard]] inline auto data() const -> const std::byte * { return buffer_; }

  /// \brief Return the allocated capacity of the buffer.
  [[nodiscard]] inline auto capacity() const -> size_t { return capacity_; }

  /// \brief Return the number of valid bytes in the buffer.
  [[nodiscard]] inline auto size() const -> size_t { return size_; }

  /// \brief Return whether the buffer is empty.
  [[nodiscard]] inline auto empty() const -> bool { return size_ == 0; }

  [[nodiscard]] inline auto first() const -> Seq { return seq_first_last.first; }
  [[nodiscard]] inline auto last() const -> Seq { return seq_first_last.second; }
  [[nodiscard]] inline auto seq_tracked() const -> const std::vector<Seq> & { return seq_tracked_; }
  [[nodiscard]] inline auto num_jsons() const -> size_t { return last() - first() + 1; }

  /// \brief Modify the number of valid of bytes in the buffer without bounds checking.
  inline void SetSizeUnsafe(size_t size) { size_ = size; }

  /// \brief Modify the number of valid bytes in the buffer with bounds checking.
  auto SetSize(size_t size) -> Status;

  /// \brief Reset the buffer.
  void Reset();

  RawJSONBuffer() = default;
 protected:
  RawJSONBuffer(std::byte *buffer, size_t capacity)
      : buffer_(buffer), capacity_(capacity) {}
  /// A pointer to the buffer.
  std::byte *buffer_ = nullptr;
  /// The number of valid bytes in the buffer.
  size_t size_ = 0;
  /// The capacity of the buffer.
  size_t capacity_ = 0;
  /// The JSONs sequence numbers contained within the buffer.
  std::pair<Seq, Seq> seq_first_last;
  /// The JSONs sequence numbers that are latency tracked.
  std::vector<Seq> seq_tracked_;
};

/// A streaming client using the Raw protocol that receives the JSONs over multiple
/// buffers with mutexes. It applies a non-blocking round-robin approach to finding an
/// available buffer to dump the contents in, but keeps track of sequence numbers to not
/// lose order.
struct DirectBufferClient : public RawClient {
 public:
  /**
   * \brief Construct a DirectBufferClient
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
                     DirectBufferClient *out) -> Status;

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
  /// \brief Return the number of received JSONs
  [[nodiscard]] auto received() const -> size_t override { return jsons_received_; }
  /// \brief Return the number of received bytes
  [[nodiscard]] auto bytes_received() const -> size_t override { return total_bytes_received_; }
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
