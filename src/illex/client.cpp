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

#include "illex/client.h"

#include "illex/log.h"

namespace illex {

auto InitSocket(const std::string& host, uint16_t port, std::shared_ptr<Socket>* out)
    -> Status {
  // Create an endpoint.
  auto endpoint = host + ":" + std::to_string(port);
  try {
    *out = std::make_shared<Socket>(kissnet::endpoint(endpoint));
  } catch (std::exception& e) {
    return Status(Error::ClientError,
                  "Unable to create socket."
                  "\nException: " +
                      std::string(e.what()) + "\nWith: " + endpoint);
  }
  // Attempt to connect.
  spdlog::info("Client connecting to {}...", endpoint);

  try {
    auto success = (*out)->connect();
    if (!success) {
      return Status(Error::ClientError, "Unable to connect to server.");
    }
  } catch (std::exception& e) {
    return Status(Error::ClientError,
                  "Unable to connect to server."
                  "\nException: " +
                      std::string(e.what()) + "\nWith: " + endpoint);
  }

  return Status::OK();
}

}  // namespace illex