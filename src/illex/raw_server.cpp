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

#include <thread>
#include <future>
#include <string>
#include <tuple>
#include <rapidjson/prettywriter.h>
#include <concurrentqueue.h>
#include <putong/timer.h>
#include <kissnet.hpp>

#include "illex/log.h"
#include "illex/document.h"
#include "illex/arrow.h"
#include "illex/raw_server.h"

namespace illex {

namespace kn = kissnet;

auto RawServer::Create(RawProtocol protocol_options, RawServer *out) -> Status {
  out->protocol = protocol_options;
  out->server = std::make_shared<RawSocket>(kn::endpoint("0.0.0.0:" + std::to_string(out->protocol.port)));
  if (protocol_options.reuse) {
    out->server->set_reuse();
  }

  try {
    out->server->bind();
  } catch (const std::runtime_error &e) {
    return Status(Error::RawError, e.what());
  }
  out->server->listen();
  SPDLOG_DEBUG("Listening on port {}", out->protocol.port);
  return Status::OK();
}

auto RawServer::SendJSONs(const ProductionOptions &options, StreamStatistics *stats) -> Status {
  // Check for some potential misuse.
  assert(stats != nullptr);
  if (this->server == nullptr) {
    return Status(Error::RawError, "RawServer uninitialized. Use RawServer::Create().");
  }

  StreamStatistics result;
  putong::Timer t;

  // Create a concurrent queue for the JSON production threads.
  ProductionQueue production_queue;
  // Spawn production hive thread.
  std::promise<ProductionStats> production_stats;
  auto producer_stats_future = production_stats.get_future();
  auto producer = std::thread(ProductionHiveThread, options, &production_queue, std::move(production_stats));

  // Accept a client.
  SPDLOG_DEBUG("Waiting for client to connect.");
  auto client = server->accept();

  // Start a timer.
  t.Start();

  // Attempt to pull all produced messages from the production queue and send them over the socket.
  for (size_t m = 0; m < options.num_jsons; m++) {
    // Get the message
    std::string message_str;
    while (!production_queue.try_dequeue(message_str)) {
#ifndef NDEBUG
      SPDLOG_DEBUG("Nothing in queue... {}");
      std::this_thread::sleep_for(std::chrono::seconds(1));
#endif
    }
    SPDLOG_DEBUG("Popped string: {}", message_str.substr(0, message_str.find('\n')));

    // Attempt to send the message.
    auto send_result = client.send(reinterpret_cast<std::byte *>(message_str.data()), message_str.length());

    auto send_result_socket = std::get<1>(send_result);
    if (send_result_socket != kissnet::socket_status::valid) {
      producer.join();
      return Status(Error::RawError, "Socket not valid after send: " + std::to_string(send_result_socket));
    }

    result.num_messages++;
  }

  // Stop the timer.
  t.Stop();
  result.time = t.seconds();

  // Wait for the producer thread to stop, and obtain the statistics.
  producer.join();
  result.producer = producer_stats_future.get();

  return Status::OK();
}

auto RawServer::Close() -> Status {
  try {
    server->close();
  } catch (const std::exception &e) {
    return Status(Error::RawError, e.what());
  }

  return Status::OK();
}

static void LogSendStats(const StreamStatistics &result) {
  spdlog::info("Streamed {} messages in {:.4f} seconds.", result.num_messages, result.time);
  spdlog::info("  {:.1f} messages/second (avg).", result.num_messages / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(result.num_bytes * 8) / result.time * 1E-9);
}

auto RunRawServer(const RawProtocol &protocol_options,
                  const ProductionOptions &production_options,
                  bool statistics) -> Status {
  SPDLOG_DEBUG("Starting Raw server.");
  RawServer server;
  ILLEX_ROE(RawServer::Create(protocol_options, &server));

  SPDLOG_DEBUG("Streaming {} messages.", production_options.num_jsons);
  StreamStatistics stats;
  ILLEX_ROE(server.SendJSONs(production_options, &stats));

  if (statistics) {
    LogSendStats(stats);
  }

  SPDLOG_DEBUG("Raw server shutting down.");
  ILLEX_ROE(server.Close());

  return Status::OK();
}

}

