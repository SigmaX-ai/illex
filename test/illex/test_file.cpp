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

#include <filesystem>
#include <fstream>
#include <streambuf>

#include "illex/file.h"

namespace illex {

TEST(File, File) {
  auto path =
      std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
  FileOptions opts;
  opts.production.schema = arrow::schema({arrow::field("test", arrow::uint64(), false)});
  opts.production.num_jsons = 16;
  opts.production.verbose = true;
  opts.out_path = path;
  std::stringstream ss;
  RunFile(opts, &ss);
  auto str0 = ss.str();
  ASSERT_EQ(std::count(str0.begin(), str0.end(), '\n'), 16);
  ASSERT_TRUE(std::filesystem::exists(path));
  auto ifs = std::ifstream(path);
  std::string str1((std::istreambuf_iterator<char>(ifs)),
                   std::istreambuf_iterator<char>());
  ASSERT_EQ(str0, str1);
  ASSERT_TRUE(std::filesystem::remove(path));
}

TEST(File, PrettyFile) {
  auto path =
      std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
  FileOptions opts;
  opts.production.schema = arrow::schema(
      {arrow::field("a", arrow::null(), false), arrow::field("b", arrow::null(), false)});
  opts.production.num_jsons = 1;
  opts.production.verbose = true;
  opts.production.pretty = true;
  opts.out_path = path;
  std::stringstream ss;
  RunFile(opts, &ss);
  auto str0 = ss.str();
  ASSERT_EQ(str0,
            "{\n"
            "    \"a\": null,\n"
            "    \"b\": null\n"
            "}\n");
}

}  // namespace illex