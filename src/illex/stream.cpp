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

#include "illex/log.h"
#include "illex/stream.h"
#include "illex/zmq_server.h"
#include "illex/raw_server.h"

namespace illex {

auto RunStream(const StreamOptions &opt) -> Status {

  if (std::holds_alternative<ZMQProtocol>(opt.protocol)) {
    return RunZMQServer(std::get<ZMQProtocol>(opt.protocol), opt.production);
  } else {
    return RunRawServer(std::get<RawProtocol>(opt.protocol), opt.production);
  }
}

}
