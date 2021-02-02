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

#include "illex/server.h"

#include <concurrentqueue.h>
#include <putong/timer.h>
#include <rapidjson/prettywriter.h>

#include <cassert>
#include <future>
#include <kissnet.hpp>
#include <string>
#include <thread>
#include <tuple>

#include "illex/arrow.h"
#include "illex/log.h"

namespace illex {

namespace kn = kissnet;

auto Server::Create(const ServerOptions& options, Server* out) -> Status {
  assert(out != nullptr);
  out->server =
      std::make_shared<Socket>(kn::endpoint("0.0.0.0:" + std::to_string(options.port)));
  if (options.reuse_socket) {
    out->server->set_reuse();
  }
  try {
    out->server->bind();
  } catch (const std::runtime_error& e) {
    return Status(Error::ServerError, e.what());
  }
  out->server->listen();
  spdlog::info("Listening on port {}", options.port);
  return Status::OK();
}

auto Server::SendJSONs(const ProductionOptions& prod_opts,
                       const RepeatOptions& repeat_opts, StreamStatistics* stats)
    -> Status {
  // Check for some potential misuse.
  assert(stats != nullptr);
  if (this->server == nullptr) {
    return Status(Error::ServerError, "Server uninitialized. Use RawServer::Create().");
  }

  // Start producing before connection is made.
  ProductionOptions prod_opts_int = prod_opts;
  // Create a concurrent queue for the JSON production threads.
  ProductionQueue production_queue;
  // Spawn production hive thread.
  std::promise<ProductionStats> production_stats;
  auto producer_stats_future = production_stats.get_future();
  auto producer = std::thread(ProductionHiveThread, prod_opts_int, &production_queue,
                              std::move(production_stats));

  // Accept a client.
  spdlog::info("Waiting for client to connect.");
  auto client = server->accept();
  spdlog::info("Client connected.");
  spdlog::info("Streaming {} messages.", prod_opts.num_jsons);

  StreamStatistics result;
  putong::Timer t;


  do {
    // Start a timer.
    t.Start();
    // Attempt to pull all produced messages from the production queue and send them over
    // the socket.
    for (size_t m = 0; m < prod_opts.num_jsons; m++) {
      // Get the message
      std::string message_str;
      while (!production_queue.try_dequeue(message_str)) {
#ifndef NDEBUG
        // Slow this down a bit in debug.
        SPDLOG_DEBUG("Nothing in queue... {}");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
#endif
      }

      // Attempt to send the message.
      auto send_result = client.send(reinterpret_cast<std::byte*>(message_str.data()),
                                     message_str.length());

      auto send_result_socket = std::get<1>(send_result);
      if (send_result_socket != kissnet::socket_status::valid) {
        producer.join();
        return Status(Error::ServerError, "Socket not valid after send: " +
                                              std::to_string(send_result_socket));
      }

      // If verbose is enabled, also print the JSON to stdout
      if (prod_opts.verbose) {
        std::cout << message_str.substr(0, message_str.length() - 1) << std::endl;
      }

      result.num_messages++;
    }

    // Stop the timer.
    t.Stop();
    result.time = t.seconds();

    // Wait for the producer thread to stop, and obtain the statistics.
    producer.join();
    result.producer = producer_stats_future.get();
    *stats = result;

    if (repeat_opts.messages) {
      std::this_thread::sleep_for(std::chrono::milliseconds(repeat_opts.interval_ms));
    }

    // In case this is on repeat, increase the seed of the generator.
    prod_opts_int.gen.seed += 42;
  } while (repeat_opts.messages);

  return Status::OK();
}

auto Server::Close() -> Status {
  try {
    server->close();
  } catch (const std::exception& e) {
    return Status(Error::ServerError, e.what());
  }

  return Status::OK();
}

static void LogSendStats(const StreamStatistics& result) {
  spdlog::info("Streamed {} messages in {:.4f} seconds.", result.num_messages,
               result.time);
  spdlog::info("  {:.1f} messages/second (avg).", result.num_messages / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(result.num_bytes * 8) / result.time * 1E-9);
}

auto RunServer(const ServerOptions& server_options,
               const ProductionOptions& production_options,
               const RepeatOptions& repeat_options, bool statistics) -> Status {
  spdlog::info("Starting server.");
  Server server;
  ILLEX_ROE(Server::Create(server_options, &server));

  StreamStatistics stats;
  ILLEX_ROE(server.SendJSONs(production_options, repeat_options, &stats));

  if (statistics) {
    LogSendStats(stats);
  }

  spdlog::info("Server shutting down.");
  ILLEX_ROE(server.Close());

  return Status::OK();
}

}  // namespace illex
