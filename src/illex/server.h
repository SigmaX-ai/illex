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

#include <kissnet.hpp>

#include "illex/client.h"
#include "illex/document.h"
#include "illex/producer.h"
#include "illex/protocol.h"
#include "illex/status.h"

namespace illex {

/// Streaming statistics.
struct StreamMetrics {
  /// Number of messages transmitted.
  size_t num_messages = 0;
  /// Number of bytes transmitted.
  size_t num_bytes = 0;
  /// Total time spent transmitting.
  double time = 0.0;
  /// Statistics of the production facilities.
  ProductionMetrics producer;
};

/// Repeat mode options.
struct RepeatOptions {
  /// Number of times to repeat sending.
  size_t times = 1;
  /// Interval between repeated sending in milliseconds.
  size_t interval_ms = 250;
};

struct ServerOptions {
  uint16_t port = ILLEX_DEFAULT_PORT;
};

/**
 * \brief A streaming server for raw JSONs directly over TCP.
 */
class Server {
 public:
  /**
   * \brief Create a new Server to stream JSONs to a client.
   *
   * \param[in]  options Server options.
   * \param[out] out     The Server object to populate.
   * \return Status::OK() if successful, some error status otherwise.
   */
  static auto Create(const ServerOptions& options, Server* out) -> Status;

  /**
   * \brief Send JSONs using this Server.
   * \param[in] prod_opts Options for the JSON production facilities.
   * \param[in] repeat_opts Options for repeated streaming mode
   * \param[out] metrics Server statistics.
   * \return Status::OK() if successful, some error status otherwise.
   */
  auto SendJSONs(const ProducerOptions& prod_opts, const RepeatOptions& repeat_opts,
                 StreamMetrics* metrics) -> Status;

  /**
   * \brief Close the Server.
   * \return Status::OK() if successful, some error status otherwise.
   */
  auto Close() -> Status;

 private:
  std::shared_ptr<Socket> server;
};

/**
 * \brief Use a RawServer to stream the specified JSONs out.
 * \param server_options     Server connection options.
 * \param production_options JSON production options.
 * \param repeat_options     Options related to how to repeat server functionality.
 * \param statistics         Whether to measure and log statistics.
 * \return Status::OK if successful, some error otherwise.
 */
auto RunServer(const ServerOptions& server_options,
               const ProducerOptions& production_options,
               const RepeatOptions& repeat_options, bool statistics) -> Status;

}  // namespace illex
