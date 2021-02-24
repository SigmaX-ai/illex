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

#include <gtest/gtest.h>

#include <memory>

#include "illex/producer.h"

namespace illex::test {

TEST(Producer, Producer) {
  ProducerOptions opts;
  opts.gen.seed = 0;
  opts.num_batches = 4;
  opts.num_jsons = 4;
  std::promise<ProductionMetrics> metrics;
  auto metrics_f = metrics.get_future();

  std::vector<std::string> keys = {"illex_MIN", "illex_MAX"};
  std::vector<std::string> values = {"0", "9"};
  auto meta = std::make_shared<arrow::KeyValueMetadata>(keys, values);
  opts.schema =
      arrow::schema({arrow::field("test", arrow::uint64(), false)->WithMetadata(meta)});
  std::atomic<bool> shutdown = false;

  ProductionQueue queue(opts.num_batches, 1, 0);
  ProductionThread(0, opts, opts.num_batches, opts.num_jsons, &queue, &shutdown,
                   std::move(metrics));
  JSONBatch test;
  // Pull all batches from the queue.
  for (size_t i = 0; i < opts.num_batches; i++) {
    ASSERT_TRUE(queue.try_dequeue(test));
  }

  // And if it returned the right number of produced bytes.
  ASSERT_EQ(metrics_f.get().num_chars,
            opts.num_batches * opts.num_jsons * strlen("{\"test\":0}\n"));

  ASSERT_FALSE(queue.try_dequeue(test));
}

}  // namespace illex::test