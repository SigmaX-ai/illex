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

#include "illex/client_buffering.h"
#include "illex/client_queueing.h"
#include "illex/stream.h"

namespace illex {

#define FAIL_ON_ERROR(status)                   \
  {                                             \
    auto __status = (status);                   \
    if (!__status.ok()) {                       \
      throw std::runtime_error(__status.msg()); \
    }                                           \
  }

auto Cast(const char* str) -> std::byte* {
  return const_cast<std::byte*>(reinterpret_cast<const std::byte*>(str));
}

void GetResultFrom(const char* str, std::pair<size_t, size_t>* result) {
  auto* buf = Cast(str);
  JSONBuffer b;
  ASSERT_TRUE(JSONBuffer::Create(buf, strlen(str), &b).ok());
  ASSERT_TRUE(b.SetSize(strlen(str)).ok());
  *result = b.Scan(strlen(str), 0);
}

TEST(Client, Scan) {
  std::pair<size_t, size_t> result;
  GetResultFrom("{}\n", &result);
  ASSERT_EQ(result.first, 1);
  ASSERT_EQ(result.second, 0);
  GetResultFrom("{}\n{}", &result);
  ASSERT_EQ(result.first, 1);
  ASSERT_EQ(result.second, 2);
  GetResultFrom("{}\n\n", &result);
  ASSERT_EQ(result.first, 1);
  ASSERT_EQ(result.second, 0);
  GetResultFrom("\n\n\n", &result);
  ASSERT_EQ(result.first, 0);
  ASSERT_EQ(result.second, 0);
  GetResultFrom("{}", &result);
  ASSERT_EQ(result.first, 0);
  ASSERT_EQ(result.second, 2);
}

void RunStreamThread(const StreamOptions& opts, Status* status) {
  *status = RunStream(opts);
}

TEST(Client, Queueing) {
  Status server_status;
  StreamOptions opts;
  opts.production.schema = arrow::schema({arrow::field("test", arrow::uint64(), false)});
  auto server = std::thread(RunStreamThread, opts, &server_status);

  QueueingClient client;
  JSONQueue client_queue;
  FAIL_ON_ERROR(QueueingClient::Create(ClientOptions(), &client_queue, &client));

  FAIL_ON_ERROR(client.ReceiveJSONs());
  FAIL_ON_ERROR(client.Close());

  server.join();
  FAIL_ON_ERROR(server_status);

  JSONItem item;
  ASSERT_TRUE(client_queue.try_dequeue(item));
  ASSERT_EQ(client.jsons_received(), 1);
}

void ConsumeBufferThread(JSONBuffer* buffer, std::mutex* mutex) {
  while (true) {
    if (!buffer->empty()) {
      std::lock_guard<std::mutex> lg(*mutex);
      ASSERT_EQ(buffer->num_jsons(), 1);
      buffer->Reset();
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

TEST(Client, Buffering) {
  Status server_status;
  StreamOptions opts;
  opts.production.schema = arrow::schema({arrow::field("test", arrow::uint64(), false)});
  auto server = std::thread(RunStreamThread, opts, &server_status);

  BufferingClient client;
  JSONBuffer buffer;
  std::mutex mutex;

  auto* raw_buffer = new std::byte[ILLEX_DEFAULT_TCP_BUFSIZE];
  FAIL_ON_ERROR(JSONBuffer::Create(raw_buffer, ILLEX_DEFAULT_TCP_BUFSIZE, &buffer));

  auto consumer = std::thread(ConsumeBufferThread, &buffer, &mutex);

  std::vector vbp = {&buffer};
  std::vector vmp = {&mutex};
  FAIL_ON_ERROR(BufferingClient::Create(ClientOptions(), vbp, vmp, &client));
  FAIL_ON_ERROR(client.ReceiveJSONs());
  FAIL_ON_ERROR(client.Close());

  consumer.join();

  server.join();
  FAIL_ON_ERROR(server_status);

  delete[] raw_buffer;

  ASSERT_EQ(client.jsons_received(), 1);
}

}  // namespace illex