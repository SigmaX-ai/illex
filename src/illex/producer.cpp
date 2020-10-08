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

#include <cstdint>
#include <rapidjson/prettywriter.h>
#include <putong/timer.h>

#include "illex/log.h"
#include "illex/arrow.h"
#include "illex/producer.h"

namespace illex {

namespace rj = rapidjson;

void ProductionDroneThread(size_t thread_id,
                           const ProductionOptions &opt,
                           size_t num_items,
                           ProductionQueue *q,
                           std::promise<size_t> &&size) {
  // Accumulator for total number of characters generated.
  size_t drone_size = 0;

  // Generation options. We increment the seed by the thread id, so we get different values from each thread.
  auto gen_opt = opt.gen;
  gen_opt.seed += thread_id;

  // Generate a message with tweets in JSON format.
  auto gen = FromArrowSchema(*opt.schema, gen_opt);
  rapidjson::StringBuffer buffer;

  for (size_t m = 0; m < num_items; m++) {
    auto json = gen.Get();
    buffer.Clear();

    // Check whether we must pretty-prent the JSON
    if (opt.pretty) {
      rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
      writer.SetFormatOptions(rj::PrettyFormatOptions::kFormatSingleLineArray);
      json.Accept(writer);
    } else {
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      json.Accept(writer);
    }

    // Put the JSON string in the queue.
    auto json_str = std::string(buffer.GetString());

    // Check if we need to append whitespace.
    if (opt.whitespace) {
      json_str.push_back(opt.whitespace_char);
    }

    // Accumulate the number of characters this drone has produced.
    drone_size += json_str.size();

    // Place the JSON in the queue.
    if (!q->enqueue(std::move(json_str))) {
      spdlog::error("Drone {} could not place JSON string in queue.", thread_id);
      // TODO(johanpel): allow threads to return with an error state.
    }
  }
  SPDLOG_DEBUG("Drone {} done.", thread_id);
  size.set_value(drone_size);
}

void ProductionHiveThread(const ProductionOptions &opt,
                          ProductionQueue *q,
                          std::promise<ProductionStats> &&stats) {
  putong::Timer t;
  ProductionStats result;

  // Number of JSONs to produce per thread
  const auto jsons_per_thread = opt.num_jsons / opt.num_threads;

  // Remainder for the first thread.
  size_t remainder = 0;
  if (jsons_per_thread == 0) {
    remainder = opt.num_jsons;
  } else {
    remainder = opt.num_jsons % jsons_per_thread;
  }

  SPDLOG_DEBUG("Starting {} JSON producer drones.", opt.num_threads);

  // Set up some vectors for the threads and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<size_t>> futures;
  threads.reserve(opt.num_threads);
  futures.reserve(opt.num_threads);

  // Spawn threads
  t.Start();
  for (int thread = 0; thread < opt.num_threads; thread++) {
    // Set up promise for the number of characters produced
    std::promise<size_t> promise_num_chars;
    futures.push_back(promise_num_chars.get_future());
    // Spawn the threads and let the first thread do the remainder of the work.
    threads.emplace_back(ProductionDroneThread,
                         thread,
                         opt,
                         jsons_per_thread + (thread == 0 ? remainder : 0),
                         q,
                         std::move(promise_num_chars));
  }

  // Wait for all threads to complete.
  for (auto &thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  t.Stop();

  // Get all futures and calculate total number of characters generated.
  size_t total_size = 0;
  for (auto &f : futures) {
    total_size += f.get();
  }
  result.time = t.seconds();

  // Print some stats.
  spdlog::info("  Produced {} JSONs in {:.4f} seconds.", opt.num_jsons, result.time);
  spdlog::info("  {:.1f} JSONs/second (avg).", opt.num_jsons / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(total_size * 8) / result.time * 1E-9);
  stats.set_value(result);
}

}
