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

#include <iostream>
#include <fstream>
#include <rapidjson/prettywriter.h>

#include "illex/document.h"
#include "illex/file.h"
#include "illex/arrow.h"
#include "illex/status.h"

namespace illex {

// TODO(johanpel): convert file generation to make use of production facilities in producer.h

auto RunFile(const FileOptions &opt) -> Status {
  // Generate the document:
  auto gen = FromArrowSchema(*opt.production.schema, opt.production.gen);
  auto json = gen.Get();

  // Write it to a StringBuffer
  rapidjson::StringBuffer buffer;
  if (opt.production.pretty) {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetFormatOptions(rj::PrettyFormatOptions::kFormatSingleLineArray);
    json.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json.Accept(writer);
  }
  const char *output = buffer.GetString();

  // Print it to stdout if requested.
  if (opt.production.verbose || opt.out_path.empty()) {
    std::cout << output << std::endl;
  }

  // Write it to a file.
  if (!opt.out_path.empty()) {
    std::ofstream of(opt.out_path);
    of << output << std::endl;
  }

  return Status::OK();
}

}
