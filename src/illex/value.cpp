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

#include "illex/value.h"

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>

#include <random>
#include <utility>

#include "illex/log.h"

namespace illex {

void Value::SetContext(Context context) {
  assert(context.engine_ != nullptr);
  assert(context.allocator_ != nullptr);
  context_ = context;
}

String::String(size_t length_min, size_t length_max)
    : length_min_(length_min), length_max_(length_max) {
  len_dist_ = UniformIntDistribution<size_t>(length_min_, length_max_);
  chars_dist_ = UniformIntDistribution<char>('a', 'z');
}

auto String::Get() -> rapidjson::Value {
  rapidjson::Value result;
  // Generate the length.
  size_t length = len_dist_(*context_.engine_);

  std::string str(length, 0);
  // Pull characters from the character distribution.
  for (char& c : str) {
    c = chars_dist_(*context_.engine_);
  }
  // Call the overload SetString with allocator to make a copy of the string.
  result.SetString(str.c_str(), str.length(), *context_.allocator_);
  return result;
}

auto DateString::Get() -> rapidjson::Value {
  rapidjson::Value result;

  // Format like ISO8601 but without the timezone
  // Apparently this is from spdlog, but we might want to import this separately.
  auto str = ::fmt::format(
      "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}{:+03d}:00", year(*context_.engine_),
      month(*context_.engine_), day(*context_.engine_), hour(*context_.engine_),
      min(*context_.engine_), sec(*context_.engine_), timezone(*context_.engine_));
  // Call the overload SetString with allocator to make a copy of the string.
  result.SetString(str.c_str(), str.length(), *context_.allocator_);
  return result;
}

DateString::DateString() {
  year = UniformIntDistribution<int64_t>(2000, 2020);
  month = UniformIntDistribution<uint8_t>(1, 12);
  day = UniformIntDistribution<uint8_t>(1, 28);
  hour = UniformIntDistribution<uint8_t>(0, 23);
  min = UniformIntDistribution<uint8_t>(0, 59);
  sec = UniformIntDistribution<uint8_t>(0, 59);
  timezone = UniformIntDistribution<int8_t>(-12, 12);
}

Array::Array(std::shared_ptr<Value> item_generator, size_t max_length, size_t min_length)
    : min_length(min_length), max_length(max_length), item_(std::move(item_generator)) {
  length = UniformIntDistribution<int32_t>(min_length, max_length);
}

auto Array::Get() -> rapidjson::Value {
  rapidjson::Value result(rapidjson::kArrayType);
  auto len = this->length(*context_.engine_);
  result.SetArray();
  result.Reserve(len, *context_.allocator_);
  for (size_t i = 0; i < len; i++) {
    result.PushBack(item_->Get(), *context_.allocator_);
  }
  return result;
}

FixedSizeArray::FixedSizeArray(size_t length, std::shared_ptr<Value> item_generator)
    : length_(length), item_(std::move(item_generator)) {}

auto FixedSizeArray::Get() -> rapidjson::Value {
  rapidjson::Value result(rapidjson::kArrayType);
  result.SetArray();
  result.Reserve(length_, *context_.allocator_);
  for (size_t i = 0; i < length_; i++) {
    result.PushBack(item_->Get(), *context_.allocator_);
  }
  return result;
}

void Member::AddTo(rapidjson::Value* object) {
  rapidjson::Value name(rapidjson::StringRef(name_.c_str()));
  rapidjson::Value val = value_->Get();
  object->AddMember(name, val, *context_.allocator_);
}

Member::Member(std::string name, std::shared_ptr<Value> value)
    : name_(std::move(name)), value_(std::move(value)) {}

Member::Member() { value_ = std::make_shared<Null>(); }

void Member::SetValue(std::shared_ptr<Value> value) {
  value_ = std::move(value);
  value_->SetContext(context_);
}

void Member::SetContext(Context context) { context_ = context; }

auto Member::context() -> Context { return context_; }

auto Member::value() const -> std::shared_ptr<Value> { return value_; }

auto Object::Get() -> rj::Value {
  rj::Value result(rj::kObjectType);
  for (auto& mg : members_) {
    mg.AddTo(&result);
  }
  return result;
}

void Object::AddMember(Member member) {
  member.SetContext(context_);
  members_.push_back(member);
}

Object::Object(const std::vector<Member>& members) {
  for (auto m : members) {
    m.SetContext(context_);
    members_.push_back(m);
  }
}

auto Null::Get() -> rj::Value { return rj::Value(rj::kNullType); }

auto Bool::Get() -> rj::Value {
  return rj::Value(((*this->context_.engine_)() % 2 == 0));
}

}  // namespace illex
