#include "nano_mooncake/observability.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>

namespace nano_mooncake {

namespace {

thread_local std::uint64_t tls_trace_id = 0;
std::atomic<std::uint64_t> g_trace_counter{1};
std::mutex g_trace_log_mu;

bool ParseEnabledEnv() {
  const char* raw = std::getenv("NANO_MOONCAKE_TRACE");
  if (raw == nullptr) {
    return false;
  }
  return raw[0] != '\0' && raw[0] != '0';
}

std::uint64_t GenerateTraceId() {
  const auto now_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  const auto seq = g_trace_counter.fetch_add(1, std::memory_order_relaxed);
  const auto tid = static_cast<std::uint64_t>(
      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  return now_ns ^ (seq << 8U) ^ (tid << 1U);
}

void AppendKeyValue(std::string& line, const char* key, std::string_view value) {
  if (value.empty()) {
    return;
  }
  line.push_back(' ');
  line += key;
  line += "=\"";
  for (char ch : value) {
    if (ch == '"' || ch == '\\') {
      line.push_back('\\');
    }
    if (ch == '\n') {
      line += "\\n";
      continue;
    }
    line.push_back(ch);
  }
  line.push_back('"');
}

void AppendUInt(std::string& line, const char* key, std::uint64_t value) {
  if (value == 0) {
    return;
  }
  char buffer[96];
  const int written =
      std::snprintf(buffer, sizeof(buffer), " %s=%llu", key,
                    static_cast<unsigned long long>(value));
  if (written > 0) {
    line.append(buffer, static_cast<std::size_t>(written));
  }
}

void AppendInt(std::string& line, const char* key, int value) {
  if (value == 0) {
    return;
  }
  char buffer[96];
  const int written = std::snprintf(buffer, sizeof(buffer), " %s=%d", key, value);
  if (written > 0) {
    line.append(buffer, static_cast<std::size_t>(written));
  }
}

}  // namespace

bool TraceEnabled() {
  static const bool enabled = ParseEnabledEnv();
  return enabled;
}

std::uint64_t CurrentTraceId() { return tls_trace_id; }

std::uint64_t EnsureTraceId() {
  if (!TraceEnabled()) {
    return 0;
  }
  if (tls_trace_id == 0) {
    tls_trace_id = GenerateTraceId();
  }
  return tls_trace_id;
}

TraceContextScope::TraceContextScope(std::uint64_t trace_id) {
  if (!TraceEnabled() || trace_id == 0) {
    return;
  }
  previous_trace_id_ = tls_trace_id;
  tls_trace_id = trace_id;
  active_ = true;
}

TraceContextScope::~TraceContextScope() {
  if (!active_) {
    return;
  }
  tls_trace_id = previous_trace_id_;
}

RootTraceScope::RootTraceScope() {
  if (!TraceEnabled() || tls_trace_id != 0) {
    return;
  }
  previous_trace_id_ = 0;
  tls_trace_id = GenerateTraceId();
  active_ = true;
}

RootTraceScope::~RootTraceScope() {
  if (!active_) {
    return;
  }
  tls_trace_id = previous_trace_id_;
}

TraceTimePoint TraceNow() { return TraceClock::now(); }

std::uint64_t ElapsedUs(TraceTimePoint start, TraceTimePoint end) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}

void LogTrace(const char* component, const char* event, const TraceFields& fields) {
  if (!TraceEnabled()) {
    return;
  }

  std::string line;
  line.reserve(320);
  line += "nm_trace";

  const auto ts_us = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  AppendUInt(line, "ts_us", ts_us);

  std::uint64_t trace_id = fields.trace_id;
  if (trace_id == 0) {
    trace_id = CurrentTraceId();
  }
  AppendUInt(line, "trace_id", trace_id);
  AppendKeyValue(line, "component", component == nullptr ? "" : component);
  AppendKeyValue(line, "event", event == nullptr ? "" : event);
  AppendKeyValue(line, "status", fields.status);
  AppendKeyValue(line, "opcode", fields.opcode);
  AppendKeyValue(line, "segment", fields.segment_name);
  AppendKeyValue(line, "endpoint", fields.endpoint);
  AppendKeyValue(line, "object_key", fields.object_key);
  AppendKeyValue(line, "message", fields.message);
  AppendUInt(line, "request_id", fields.request_id);
  AppendUInt(line, "batch_id", fields.batch_id);
  AppendUInt(line, "segment_id", fields.segment_id);
  AppendUInt(line, "offset", fields.offset);
  AppendUInt(line, "bytes", static_cast<std::uint64_t>(fields.bytes));
  AppendUInt(line, "duration_us", fields.duration_us);
  AppendUInt(line, "items", static_cast<std::uint64_t>(fields.item_count));
  AppendInt(line, "error_code", fields.error_code);
  if (fields.has_cache_hit) {
    AppendKeyValue(line, "cache_hit", fields.cache_hit ? "true" : "false");
  }
  line.push_back('\n');

  std::lock_guard<std::mutex> lock(g_trace_log_mu);
  std::fwrite(line.data(), 1, line.size(), stderr);
  std::fflush(stderr);
}

const char* TransferOpcodeName(std::uint8_t opcode_value) {
  switch (opcode_value) {
    case 0:
      return "read";
    case 1:
      return "write";
    default:
      return "unknown";
  }
}

}  // namespace nano_mooncake
