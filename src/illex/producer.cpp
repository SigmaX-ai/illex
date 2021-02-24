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

#include "illex/producer.h"

#include <putong/timer.h>
#include <rapidjson/prettywriter.h>

#include <cstdint>

#include "illex/arrow.h"
#include "illex/log.h"

namespace illex {

namespace rj = rapidjson;

void ProductionDroneThread(size_t thread_id, const ProductionOptions& opt,
                           size_t num_batches, size_t num_items, ProductionQueue* q,
                           std::promise<size_t>&& size) {
  using PrettyWriter = rapidjson::PrettyWriter<rapidjson::StringBuffer>;
  using NormalWriter = rapidjson::Writer<rapidjson::StringBuffer>;

  // Accumulator for total number of characters generated.
  size_t drone_size = 0;

  // Generation options. We increment the seed by the thread id, so we get different
  // values from each thread.
  auto gen_opt = opt.gen;
  gen_opt.seed += thread_id;

  // Set up generator.
  auto gen = FromArrowSchema(*opt.schema, gen_opt);

  // Set up RapidJSON
  rapidjson::StringBuffer buffer;
  std::shared_ptr<rapidjson::Writer<rapidjson::StringBuffer>> writer;
  if (opt.pretty) {
    auto pw = std::make_shared<PrettyWriter>(buffer);
    pw->SetFormatOptions(rj::PrettyFormatOptions::kFormatSingleLineArray);
    writer = pw;
  } else {
    writer = std::make_shared<NormalWriter>(buffer);
  }

  for (size_t b = 0; b < num_batches; b++) {
    buffer.Clear();
    // Generate num_items JSON items in the buffer.
    for (size_t m = 0; m < num_items; m++) {
      // Get a new value.
      auto json = gen.Get();
      // Reset writer and write it to the buffer.
      writer->Reset(buffer);
      if (opt.pretty) {
        json.Accept(*std::static_pointer_cast<PrettyWriter>(writer));
      } else {
        json.Accept(*writer);
      }
      // Check if we need to append whitespace.
      if (opt.whitespace) {
        buffer.Put(opt.whitespace_char);
      }
    }

    // Put the batch of JSON strings in the queue.
    auto batch = std::string(buffer.GetString());
    // Accumulate the number of bytes in the batch to all that this drone has produced.
    drone_size += batch.size();
    // Place the batch in the queue.
    while (!q->try_enqueue(batch)) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  SPDLOG_DEBUG("Drone {} done.", thread_id);
  size.set_value(drone_size);
}

auto ProduceJSONs(const ProductionOptions& opt, ProductionQueue* queue,
                  ProductionStats* stats_out) -> Status {
  assert(stats_out != nullptr);
  putong::Timer t;
  ProductionStats result;

  // Number of JSONs to produce per thread
  size_t batches_per_thread = 1;
  size_t batches_remainder = 0;
  auto jsons_per_thread = opt.num_jsons / opt.num_threads;
  // Remainder for the first thread.
  size_t jsons_remainder = 0;
  if (jsons_per_thread == 0) {
    jsons_remainder = opt.num_jsons;
  } else {
    jsons_remainder = opt.num_jsons % jsons_per_thread;
  }

  // Recalculate if batching is enabled.
  if (opt.batching) {
    jsons_per_thread = opt.num_jsons;
    batches_per_thread = opt.num_batches / opt.num_threads;
    // Remainder for the first thread.
    if (batches_per_thread == 0) {
      batches_remainder = opt.num_batches;
    } else {
      batches_remainder = opt.num_batches % batches_per_thread;
    }
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
    size_t thread_jsons = jsons_per_thread + (thread == 0 ? jsons_remainder : 0);
    size_t thread_batches = batches_per_thread + (thread == 0 ? batches_remainder : 0);

    threads.emplace_back(ProductionDroneThread, thread, opt, thread_batches, thread_jsons,
                         queue, std::move(promise_num_chars));
  }

  // Wait for all threads to complete.
  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  t.Stop();

  // Get all futures and calculate total number of characters generated.
  size_t total_size = 0;
  for (auto& f : futures) {
    total_size += f.get();
  }
  result.time = t.seconds();

  // Print some stats.
  if (opt.statistics) {
    spdlog::info("Produced {} JSONs in {:.4f} seconds.", opt.num_jsons, result.time);
    spdlog::info("  {:.1f} JSON/s (avg).", opt.num_jsons / result.time);
    spdlog::info("  {:.2f} GB/s   (avg).",
                 (static_cast<double>(total_size) * 1E-9) / result.time);
  }

  *stats_out = result;

  return Status::OK();
}

}  // namespace illex
