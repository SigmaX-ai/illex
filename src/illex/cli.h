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

#include <random>
#include <cstdint>
#include <string>
#include <CLI/CLI.hpp>
#include <arrow/api.h>

#include "./file.h"
#include "./zmq_server.h"

namespace illex {

/// @brief The subcommands that can be run.
enum class SubCommand { NONE, FILE, STREAM };

/// @brief Application options parser.
struct AppOptions {
  /// The name of the application.
  constexpr static auto name = "illex";
  /// A description of the application.
  constexpr static auto desc = "A JSON generator based on Arrow Schemas.";

  /// @brief Construct an instance of the application options parser.
  static auto FromArguments(int argc, char *argv[], AppOptions *out) -> Status;

  /// The subcommand to run.
  SubCommand sub = SubCommand::NONE;

  /// The file subcommand parameters.
  FileOptions file;
  /// The stream subcommand parameters.
  StreamOptions stream;

  /// Whether to immediately exit the application after parsing the CLI options.
  bool exit = false;
  /// The return value in case immediate exit is required.
  int return_value = 0;
};

} // namespace illex
