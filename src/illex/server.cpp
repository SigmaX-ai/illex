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

  // Create a concurrent queue for the JSON production threads.
  ProductionQueue production_queue;
  ProductionOptions prod_opts_int = prod_opts;

  // Accept a client.
  spdlog::info("Waiting for client to connect.");
  auto client = server->accept();
  spdlog::info("Client connected.");

  spdlog::info("Streaming JSONs...");
  if (repeat_opts.times > 1) {
    spdlog::info("Repeating {} times.", repeat_opts.times);
    spdlog::info("  Interval: {} ms (+ production time)", repeat_opts.interval_ms);
  }

  StreamStatistics result;
  putong::Timer t;

  bool color = false;

  for (size_t repeats = 0; repeats < repeat_opts.times; repeats++) {
    size_t num_messages = 0;
    // Produce JSONs.
    ProductionStats production_stats;
    auto produce_stats =
        ProduceJSONs(prod_opts_int, &production_queue, &production_stats);

    // Start a timer.
    t.Start();
    // Attempt to pull all produced messages from the production queue and send them over
    // the socket.
    while (num_messages != prod_opts.num_batches * prod_opts.num_jsons) {
      // Pop a message from the queue.
      std::string message;
      while (!production_queue.try_dequeue(message)) {
#ifndef NDEBUG
        // Slow this down a bit in debug.
        SPDLOG_DEBUG("Nothing in queue... {}");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
#else
        std::this_thread::sleep_for(std::chrono::microseconds(1));
#endif
      }

      // Attempt to send the message.
      auto send_result =
          client.send(reinterpret_cast<std::byte*>(message.data()), message.length());

      auto send_result_socket = std::get<1>(send_result);
      if (send_result_socket != kissnet::socket_status::valid) {
        return Status(Error::ServerError, "Socket not valid after send: " +
                                              std::to_string(send_result_socket));
      }

      // If verbose is enabled, also print the JSON to stdout. Swap colors for each batch.
      if (prod_opts.verbose) {
        std::cout << (color ? "\033[34m" : "\033[35m");
        color = !color;
        std::cout << message.substr(0, message.length() - 1) << std::endl;
        std::cout << "\033[39m";
      }

      num_messages += prod_opts.num_jsons;
    }
    // Stop the timer after sending all messages and update statistics.
    t.Stop();
    result.num_messages += num_messages;
    result.time += t.seconds();
    result.producer += production_stats;
    *stats = result;

    std::this_thread::sleep_for(std::chrono::milliseconds(repeat_opts.interval_ms));

    // In case this is on repeat, increase the seed of the generator.
    prod_opts_int.gen.seed += 42;
  }

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
