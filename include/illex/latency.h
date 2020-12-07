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

#include <cassert>
#include <chrono>

#include "illex/status.h"

namespace illex {

using Timer = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

class LatencyTracker {
 public:
  LatencyTracker(size_t num_samples, size_t num_stages, size_t sample_interval)
      : num_samples_(num_samples),
        num_stages_(num_stages),
        sample_interval_(sample_interval) {
    // Allocate a contiguous buffer to store time points.
    points_ = new TimePoint[num_samples_ * num_stages_];
  }

  ~LatencyTracker() {
    // Clean up buffer.
    delete[] points_;
  }

  /**
   * \brief Potentially place a time point in the tracker.
   *
   * The tracker will only store the time point if the sequence number modulo the sample
   * interval is zero.
   *
   * The index at which the time point is placed is the sequence number divided by the
   * sample interval, modulo the number of samples. This means that this function wraps
   * around when sequence numbers divided by the sample interval exceeds the number of
   * samples.
   *
   * This function is unsafe. When supplying an incorrect stage, it can write outside
   * internal buffer bounds.
   *
   * \param seq The sequence number.
   * \param stage The stage.
   * \param value The value to put.
   * \return True if it was put, false otherwise.
   */
  inline auto Put(size_t seq, size_t stage, TimePoint value) -> bool {
    assert(stage < num_stages_);
    if (seq % sample_interval_ == 0) {
      points_[((seq / sample_interval_) % num_samples_) * num_stages_ + stage] = value;
      return true;
    } else {
      return false;
    }
  }

  /**
   * \brief Returns a TimePoint from the latency tracker.
   *
   * This function throws a runtime error if index or stage is out of bounds.
   *
   * \param index The index of the TimePoint.
   * \param stage The stage of the TimePoint.
   * \return The TimePoint.
   */
  [[nodiscard]] inline auto Get(size_t index, size_t stage) const -> TimePoint {
    if (stage >= num_stages_) {
      throw std::runtime_error("Stage index out of bounds.");
    }
    if (index > num_samples_) {
      throw std::runtime_error("Sample index out of bounds.");
    }
    return points_[index * num_stages_ + stage];
  }

  /**
   * \brief Return an interval between a stage and its previous stage.
   * \param index The index.
   * \param stage The stage, must be higher than 0.
   * \return The duration between stage-1 and stage.
   */
  [[nodiscard]] inline auto GetInterval(size_t index, size_t stage) const -> double {
    if (stage == 0) {
      throw std::runtime_error(
          "Stage must be > 0 to obtain interval between stage and previous.");
    }
    std::chrono::duration<double> diff = Get(index, stage) - Get(index, stage - 1);
    return diff.count();
  }

  [[nodiscard]] inline auto num_samples() const -> size_t { return num_samples_; }

 private:
  size_t sample_interval_;
  size_t num_samples_;
  size_t num_stages_;
  TimePoint *points_;
};

}