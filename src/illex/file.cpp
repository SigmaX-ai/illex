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

#include "illex/file.h"

#include <fstream>
#include <iostream>

#include "illex/status.h"

namespace illex {

auto RunFile(const FileOptions& opt, std::ostream* o) -> Status {
  // Produce JSON data.
  ProductionQueue queue;
  ProductionStats stats;
  auto produce_stats = ProduceJSONs(opt.production, &queue, &stats);

  // Open file for writing, if required.
  std::ofstream ofs;
  if (!opt.out_path.empty()) {
    ofs.open(opt.out_path);
    if (!ofs.good()) {
      return Status(Error::IOError, "Could not open " + opt.out_path + " for writing.");
    }
  }

  // Dump all JSONs.
  std::string json;
  while (queue.try_dequeue(json)) {
    // Print it to stdout if requested.
    if (opt.production.verbose || opt.out_path.empty()) {
      (*o) << json;
    }

    // Write it to a file.
    if (!opt.out_path.empty()) {
      ofs << json;
    }
  }

  return Status::OK();
}

}  // namespace illex
