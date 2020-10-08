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

#include <string>
#include <utility>

#include <kissnet.hpp>
#include <putong/timer.h>

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
   * \param[in] protocol The protocol options.
   * \param[in] host The hostname to connect to.
   * \param[in] seq Starting sequence number.
   * \param[out] out The raw client that will be populated by this function.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(RawProtocol protocol, std::string host, uint64_t seq, RawClient *out) -> Status;

  /**
   * \brief Receive JSONs on this raw stream client and put them in queue.
   * \param queue           The queue to put the JSONs in.
   * \param latency_timer   A timer that is started on arrival of the TCP packet. Ignored when it is nullptr. (TODO)
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(JSONQueue *queue, putong::Timer<> *latency_timer = nullptr) -> Status;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Close() -> Status;

  /// \brief Return the number of received JSONs
  [[nodiscard]] auto received() const -> size_t { return received_; }

 private:
  /// The next available sequence number.
  uint64_t seq = 0;
  /// The number of received JSONs.
  size_t received_ = 0;
  /// The host name to connect to.
  std::string host = "localhost";
  /// The protocol options.
  illex::RawProtocol protocol;
  /// The TCP socket.
  std::shared_ptr<RawSocket> client;

};

}
