// Copyright 2020 Delft University of Technology
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

#include <zmqpp/zmqpp.hpp>
#include <arrow/api.h>
#include <illex/protocol.h>

#include "illex/status.h"
#include "illex/document.h"
#include "illex/producer.h"

namespace illex {

/// @brief Options for the stream subcommand.
struct StreamOptions {
  /// Properties of the message protocol.
  StreamProtocol protocol;
  /// Options for the JSON production facilities.
  ProductionOptions production;
  /// Whether to log statistics
  bool statistics = true;
  /// Repeat indefinitely
  bool repeat = true;
};

/// @brief Streaming statistics.
struct StreamStatistics {
  /// Number of messages transmitted.
  size_t num_messages = 0;
  /// Number of bytes transmitted.
  size_t num_bytes = 0;
  /// Total time spent transmitting.
  double time = 0.0;
  /// Statistics of the production facilities.
  ProductionStats producer;
};

/// @brief Run the stream subcommand.
auto RunStream(const StreamOptions &options) -> Status;

}
