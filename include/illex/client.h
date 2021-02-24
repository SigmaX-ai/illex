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

#include "illex/latency.h"
#include "illex/protocol.h"
#include "illex/status.h"

namespace illex {

/// TCP default receive buffer size.
#define ILLEX_DEFAULT_TCP_BUFSIZE (16 * 1024 * 1024)

/// TCP default port.
#define ILLEX_DEFAULT_PORT 10197

/// Buffer sequence number.
using Seq = uint64_t;

/// A TCP socket.
using Socket = kissnet::socket<kissnet::protocol::tcp>;

/// Basic options for client implementations.
struct ClientOptions {
  /// The hostname to connect to.
  std::string host = "localhost";
  /// The port to connect to.
  uint16_t port = ILLEX_DEFAULT_PORT;
  /// The starting sequence number of the first JSON received.
  uint64_t seq = 0;
  /// Protocol options
  Protocol protocol = {};
};

/// Abstract class for client implementations.
class Client {
 public:
  /**
   * \brief Receive JSONs on this raw stream client and put them in a queue.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto ReceiveJSONs(LatencyTracker* lat_tracker = nullptr) -> Status = 0;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Close() -> Status = 0;

  /// \brief Return the number of received JSONs.
  [[nodiscard]] virtual auto jsons_received() const -> size_t = 0;

  /// \brief Return the number of received bytes.
  [[nodiscard]] virtual auto bytes_received() const -> size_t = 0;
};

/**
 * \brief Initialize a socket, by creating it and attempting to connect.
 * \param[in]  host The host to connect to.
 * \param[in]  port The port to connect to.
 * \param[out] out  A shared ptr to the created socket, if successful.
 * \return Status::OK() if successful, some error otherwise.
 */
auto InitSocket(const std::string& host, uint16_t port, std::shared_ptr<Socket>* out)
    -> Status;

}  // namespace illex
