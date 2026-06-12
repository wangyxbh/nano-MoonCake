#pragma once

#include <chrono>
#include <cstdint>
#include <cstddef>
#include <string_view>

namespace nano_mooncake {

using TraceClock = std::chrono::steady_clock;
using TraceTimePoint = TraceClock::time_point;

struct TraceFields {
  std::uint64_t trace_id = 0;
  std::uint64_t request_id = 0;
  std::uint64_t batch_id = 0;
  std::uint64_t segment_id = 0;
  std::uint64_t offset = 0;
  std::size_t bytes = 0;
  std::uint64_t duration_us = 0;
  std::size_t item_count = 0;
  int error_code = 0;
  bool cache_hit = false;
  bool has_cache_hit = false;
  std::string_view status;
  std::string_view opcode;
  std::string_view segment_name;
  std::string_view endpoint;
  std::string_view object_key;
  std::string_view message;
};

bool TraceEnabled();
std::uint64_t CurrentTraceId();
std::uint64_t EnsureTraceId();

class TraceContextScope {
 public:
  explicit TraceContextScope(std::uint64_t trace_id);
  ~TraceContextScope();

  TraceContextScope(const TraceContextScope&) = delete;
  TraceContextScope& operator=(const TraceContextScope&) = delete;

 private:
  bool active_ = false;
  std::uint64_t previous_trace_id_ = 0;
};

class RootTraceScope {
 public:
  RootTraceScope();
  ~RootTraceScope();

  RootTraceScope(const RootTraceScope&) = delete;
  RootTraceScope& operator=(const RootTraceScope&) = delete;

  bool active() const { return active_; }

 private:
  bool active_ = false;
  std::uint64_t previous_trace_id_ = 0;
};

TraceTimePoint TraceNow();
std::uint64_t ElapsedUs(TraceTimePoint start, TraceTimePoint end);
void LogTrace(const char* component, const char* event,
              const TraceFields& fields = {});

const char* TransferOpcodeName(std::uint8_t opcode_value);

}  // namespace nano_mooncake
