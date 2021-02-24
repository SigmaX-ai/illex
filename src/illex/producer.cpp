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

void ProductionThread(size_t thread_id, const ProducerOptions& opt, size_t num_batches,
                      size_t num_items, ProductionQueue* queue,
                      std::atomic<bool>* shutdown,
                      std::promise<ProductionMetrics>&& metrics_promise) {
  using PrettyWriter = rapidjson::PrettyWriter<rapidjson::StringBuffer>;
  using NormalWriter = rapidjson::Writer<rapidjson::StringBuffer>;

  putong::Timer<> t(true);

  ProductionMetrics metrics;

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
      metrics.num_jsons++;
    }

    // Put the batch of JSON strings in the queue.
    auto batch = JSONBatch{std::string(buffer.GetString()), num_items};
    // Accumulate the number of bytes in the batch to all that this drone has produced.
    metrics.num_chars += batch.data.size();
    metrics.num_batches++;
    // Place the batch in the queue.
    while (!queue->try_enqueue(batch) && !shutdown->load()) {
      metrics.queue_full++;
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  t.Stop();
  metrics.time = t.seconds();

  metrics_promise.set_value(metrics);
}

auto Producer::Make(const ProducerOptions& opt, ProductionQueue* queue,
                    std::shared_ptr<Producer>* out) -> Status {
  auto result = std::shared_ptr<Producer>(new Producer());
  result->opts_ = opt;
  result->queue_ = queue;
  *out = result;
  return Status::OK();
}

auto Producer::Start(std::atomic<bool>* shutdown) -> Status {
  assert(shutdown != nullptr);
  // Number of JSONs to produce per thread
  size_t batches_per_thread = 1;
  size_t batches_remainder = 0;
  auto jsons_per_thread = opts_.num_jsons / opts_.num_threads;
  // Remainder for the first thread.
  size_t jsons_remainder = 0;
  if (jsons_per_thread == 0) {
    jsons_remainder = opts_.num_jsons;
  } else {
    jsons_remainder = opts_.num_jsons % jsons_per_thread;
  }

  // Recalculate if batching is enabled.
  if (opts_.batching) {
    jsons_per_thread = opts_.num_jsons;
    batches_per_thread = opts_.num_batches / opts_.num_threads;
    // Remainder for the first thread.
    if (batches_per_thread == 0) {
      batches_remainder = opts_.num_batches;
    } else {
      batches_remainder = opts_.num_batches % batches_per_thread;
    }
  }

  SPDLOG_DEBUG("Starting {} JSON producer threads.", opts_.num_threads);

  threads_.reserve(opts_.num_threads);
  thread_metrics_.reserve(opts_.num_threads);

  // Spawn threads
  for (int thread = 0; thread < opts_.num_threads; thread++) {
    // Set up promise for the number of characters produced
    std::promise<ProductionMetrics> metrics_promise;
    thread_metrics_.push_back(metrics_promise.get_future());

    // Spawn the threads and let the first thread do the remainder of the work.
    size_t thread_jsons = jsons_per_thread + (thread == 0 ? jsons_remainder : 0);
    size_t thread_batches = batches_per_thread + (thread == 0 ? batches_remainder : 0);

    threads_.emplace_back(ProductionThread, thread, opts_, thread_batches, thread_jsons,
                          queue_, shutdown, std::move(metrics_promise));
  }

  return Status::OK();
}

auto Producer::Finish() -> Status {
  // Wait for all threads to complete.
  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  // Get all futures and calculate total number of characters generated.
  for (auto& f : thread_metrics_) {
    metrics_ += f.get();
  }

  return Status::OK();
}

void ProductionMetrics::Log(size_t threads) const {
  double t_avg = time / threads;
  spdlog::info("Produced {} batches of {} JSONs.", num_batches, num_jsons);
  spdlog::info("Spent average of {:.4f} seconds/thread in {} threads.", t_avg, threads);
  spdlog::info("  {:.1f} JSON/s (avg).", num_jsons / t_avg);
  spdlog::info("  {:.2f} GB/s   (avg).", (static_cast<double>(num_chars) * 1E-9) / t_avg);
  spdlog::info("  Queue was full {} times.", queue_full);
}

}  // namespace illex
