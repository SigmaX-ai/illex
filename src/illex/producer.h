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

#include <cstdint>
#include <future>

#include "illex/document.h"
#include "illex/status.h"

namespace illex {

using ProductionQueue = moodycamel::BlockingConcurrentQueue<std::string>;

/// Options for the random JSON production facility.
struct ProductionOptions {
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
};

/// Statistics on JSON production.
struct ProductionStats {
  /// The time spent producing all JSONs
  double time = 0.0;

  auto operator+=(const ProductionStats& rhs) -> ProductionStats& {
    this->time += rhs.time;
    return *this;
  }
};

/**
 * \brief A thread producing JSONs
 * \param thread_id   The ID of this thread.
 * \param opt         Production options for this thread.
 * \param num_batches Number of batches to produce.
 * \param num_items   Number of JSONs to produce per batch.
 * \param q           The queue to store the produced JSONs in.
 * \param size        The number of characters generated.
 */
void ProductionDroneThread(size_t thread_id, const ProductionOptions& opt,
                           size_t num_batches, size_t num_items, ProductionQueue* q,
                           std::promise<size_t>&& size);

/**
 * \brief Produce JSONs and push them onto a queue.
 * \param[in]  opt       Options related to how to produce the JSONs
 * \param[out] queue     The concurrent queue to operate on.
 * \param[out] stats_out Statistics about producing JSONs.
 * \returns Status::OK() if successful, some error otherwise.
 */
auto ProduceJSONs(const ProductionOptions& opt, ProductionQueue* queue,
                  ProductionStats* stats_out) -> Status;

}  // namespace illex