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

#include <putong/status.h>

#include <iostream>

namespace illex {

#define ILLEX_ROE(s)                     \
  {                                      \
    auto __status = (s);                 \
    if (!__status.ok()) return __status; \
  }

enum class Error {
  GenericError,  ///< Generic errors.
  CLIError,      ///< Errors related to the CLI.
  ServerError,   ///< Errors related to the stream mode server.
  ClientError,   ///< Errors related to the stream mode client.
  IOError        ///< Errors related to file I/O.
};

using Status = putong::Status<Error>;

}  // namespace illex