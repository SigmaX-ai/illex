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

#include <arrow/api.h>
#include <blockingconcurrentqueue.h>
#include <putong/timer.h>

#include <cstdint>
#include <future>
#include <kissnet.hpp>
#include <string>
#include <utility>

#include "illex/client.h"
#include "illex/client_queueing.h"
#include "illex/document.h"
#include "illex/latency.h"
#include "illex/log.h"
#include "illex/protocol.h"
#include "illex/status.h"

namespace illex {

/// A single JSON item
struct JSONItem {
  /// Sequence number.
  Seq seq = 0;
  /// Raw JSON string.
  std::string string;
};

/// A JSON queue for downstream tools.
using JSONQueue = moodycamel::BlockingConcurrentQueue<JSONItem>;

/**
 * \brief A client that attempts to immediately queue received JSONs.
 */
struct QueueingClient : public Client {
 public:
  ~QueueingClient();

  static auto Create(const ClientOptions& options, JSONQueue* queue, QueueingClient* out,
                     size_t buffer_size = ILLEX_DEFAULT_TCP_BUFSIZE) -> Status;

  /**
   * \brief Receive JSONs on this raw stream client and put them in a queue.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(LatencyTracker* lat_tracker = nullptr) -> Status override;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Close() -> Status override;

  /// \brief Return the number of received JSONs
  [[nodiscard]] auto jsons_received() const -> size_t override { return received_; }

  /// \brief Return the number of received bytes
  [[nodiscard]] auto bytes_received() const -> size_t override { return bytes_received_; }

 private:
  // TCP receive buffer.
  std::byte* buffer = nullptr;
  // TCP receive buffer size.
  size_t buffer_size = ILLEX_DEFAULT_TCP_BUFSIZE;
  // Whether the client must be closed.
  bool must_be_closed = false;
  // The queue to dump JSONs in.
  JSONQueue* queue = nullptr;
  /// The next available sequence number.
  Seq seq = 0;
  /// The number of received JSONs.
  size_t received_ = 0;
  /// The number of received bytes.
  size_t bytes_received_ = 0;
  /// The TCP socket.
  std::shared_ptr<Socket> client = nullptr;
};

}  // namespace illex
