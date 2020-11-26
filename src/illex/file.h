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
#include <string>

#include "illex/status.h"
#include "illex/document.h"
#include "illex/producer.h"

namespace illex {

/// \brief Options for the file subcommand.
struct FileOptions {
  /// Production options
  ProductionOptions production;
  /// The output file path.
  std::string out_path;
};

/// \brief Run the file subcommand.
auto RunFile(const FileOptions &opt) -> Status;

}
