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

#include <arrow/api.h>
#include <blockingconcurrentqueue.h>
#include <putong/timer.h>

#include <cstdint>
#include <future>

#include "illex/document.h"
#include "illex/status.h"

namespace illex {

struct JSONBatch {
  std::string data;
  size_t num_jsons;
};

using ProductionQueue = moodycamel::BlockingConcurrentQueue<JSONBatch>;

/// Options for the Producer.
struct ProducerOptions {
  /// Random generation options.
  GenerateOptions gen;
  /// The Arrow schema to base the JSONs on.
  std::shared_ptr<arrow::Schema> schema = nullptr;
  /// The number of JSONs to produce.
  size_t num_jsons = 1;
  /// Whether to insert a whitespace after a JSON.
  bool whitespace = true;
  /// The whitespace character to insert.
  char whitespace_char = '\n';
  /// Whether to print all generated JSONs to stdout.
  bool verbose = false;
  /// Whether to print statistics.
  bool statistics = false;
  /// Whether to pretty-print the JSONs.
  bool pretty = false;
  /// Number of production threads to spawn.
  size_t num_threads = 1;
  /// Produce JSONs in batches, this causes every batch to hold num_jsons.
  bool batching = false;
  /// Number of batches to produce.
  size_t num_batches = 1;
  /// Produced JSON batches queue size.
  size_t queue_size = 32;
};

/// Metrics on JSON production.
struct ProductionMetrics {
  /// The time spent producing all JSONs
  double time = 0.0;
  /// The number of characters produced.
  size_t num_chars = 0;
  /// The number of JSONs produced;
  size_t num_jsons = 0;
  /// The number of batches produced;
  size_t num_batches = 0;
  /// Number of times the production queue was full.
  size_t queue_full = 0;

  inline auto operator+=(const ProductionMetrics& rhs) -> ProductionMetrics& {
    time += rhs.time;
    num_chars += rhs.num_chars;
    num_jsons += rhs.num_jsons;
    queue_full += rhs.queue_full;
    num_batches += rhs.num_batches;
    return *this;
  }

  void Log(size_t num_threads) const;
};

/// Producer managing concurrent production of random JSONs.
class Producer {
 public:
  /**
   * \brief Create a JSON producer.
   * \param opt   The production options.
   * \param queue The queue to produce JSON batches into.
   * \param out   A shared ptr in which to store the producer.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Make(const ProducerOptions& opt, ProductionQueue* queue,
                   std::shared_ptr<Producer>* out) -> Status;

  /**
   * \brief Start the producer, spawning its threads in the background. Non-blocking.
   * \param shutdown A signal for threads they need to shut down early, can be asserted
   *                 by the threads of this producer.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Start(std::atomic<bool>* shutdown) -> Status;

  /**
   * \brief Stop the producer, joining all threads once they are finished.
   *
   * If for some reason the producer needs to stop early, assert the shutdown signal
   * before calling this.
   *
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Finish() -> Status;

  /// Return the accumulated metrics of the production threads.
  [[nodiscard]] auto metrics() const -> ProductionMetrics { return metrics_; }

 private:
  Producer() = default;
  ProducerOptions opts_;
  std::vector<std::thread> threads_;
  std::vector<std::future<ProductionMetrics>> thread_metrics_;
  ProductionMetrics metrics_;
  ProductionQueue* queue_ = nullptr;
};

/**
 * \brief A thread producing JSONs
 * \param thread_id   The ID of this thread.
 * \param opt         Production options for this thread.
 * \param num_batches Number of batches to produce.
 * \param num_items   Number of JSONs to produce per batch.
 * \param q           The queue to store the produced JSONs in.
 * \param shutdown    Shutdown signal in case other threads encountered errors.
 * \param size        Production metrics from this single thread.
 */
void ProductionThread(size_t thread_id, const ProducerOptions& opt, size_t num_batches,
                      size_t num_items, ProductionQueue* queue,
                      std::atomic<bool>* shutdown,
                      std::promise<ProductionMetrics>&& metrics_promise);

/**
 * \brief Produce JSONs and push them onto a queue.
 * \param[in]  opt       Options related to how to produce the JSONs
 * \param[out] queue     The concurrent queue to operate on.
 * \param[out] stats_out Statistics about producing JSONs.
 * \returns Status::OK() if successful, some error otherwise.
 */
auto ProduceJSONs() -> Status;

}  // namespace illex