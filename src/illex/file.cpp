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

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include "illex/status.h"

namespace illex {

auto RunFile(const FileOptions& opt, std::ostream* o) -> Status {
  // Produce JSON data.
  ProductionQueue queue(1, opt.production.num_threads, 0);

  std::atomic<bool> shutdown = false;
  std::shared_ptr<Producer> producer;
  ILLEX_ROE(Producer::Make(opt.production, &queue, &producer));
  producer->Start(&shutdown);

  // Open file for writing, if required.
  std::ofstream ofs;
  if (!opt.out_path.empty()) {
    ofs.open(opt.out_path);
    if (!ofs.good()) {
      return Status(Error::IOError, "Could not open " + opt.out_path + " for writing.");
    }
  }

  // Dump all JSONs.
  size_t num_jsons = 0;
  JSONBatch batch;
  while ((num_jsons < (opt.production.num_batches * opt.production.num_jsons)) &&
         !shutdown.load()) {
    if (queue.try_dequeue(batch)) {
      // Print it to stdout if requested.
      if (opt.production.verbose || opt.out_path.empty()) {
        (*o) << batch.data;
      }
      // Write it to a file.
      if (!opt.out_path.empty()) {
        ofs << batch.data;
      }
      num_jsons += batch.num_jsons;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  producer->Finish();

  if (!opt.out_path.empty()) {
    producer->metrics().Log(opt.production.num_threads);
  }

  return Status::OK();
}

}  // namespace illex
