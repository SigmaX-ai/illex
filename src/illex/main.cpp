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

#include "illex/cli.h"
#include "illex/file.h"
#include "illex/log.h"
#include "illex/stream.h"

auto main(int argc, char* argv[]) -> int {
  // Set up logger.
  illex::StartLogger();

  // Parse command-line arguments:
  illex::AppOptions opt;
  auto status = illex::AppOptions::FromArguments(argc, argv, &opt);
  if (status.ok()) {
    // Run the requested sub-program:
    switch (opt.sub) {
      case illex::SubCommand::NONE:
        break;
      case illex::SubCommand::FILE:
        status = illex::RunFile(opt.file);
        break;
      case illex::SubCommand::STREAM:
        status = illex::RunStream(opt.stream);
        break;
    }
  }

  if (!status.ok()) {
    spdlog::error("{} exiting with errors.", illex::AppOptions::name);
    spdlog::error("  {}", status.msg());
    return -1;
  }

  return 0;
}
