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

#include <string>
#include <utility>

#include <kissnet.hpp>

#include "illex/log.h"
#include "illex/status.h"
#include "illex/queue.h"
#include "illex/raw_protocol.h"

namespace illex {

/// A streaming client using the Raw protocol.
struct RawClient {
 public:
  /**
   * \brief Construct a new raw client.
   * \param protocol The protocol options.
   * \param host The hostname to connect to.
   * \param out The raw client that will be populated by this function.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(RawProtocol protocol, std::string host, RawClient *out) -> Status;

  /**
   * \brief Receive JSONs on this raw stream client and put them in queue.
   * \param queue The queue to put the JSONs in.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(Queue *queue) -> Status;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Close() -> Status;

  /// \brief Return the number of received JSONs
  [[nodiscard]] auto received() const -> size_t { return received_; }

 private:
  size_t received_ = 0;
  std::string host = "localhost";
  illex::RawProtocol protocol;
  std::shared_ptr<RawSocket> client;

};

}  // namespace flitter
