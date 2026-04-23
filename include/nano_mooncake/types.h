#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace nano_mooncake {

enum class DeviceType : uint8_t {
  kCPU = 0,
  kCUDA = 1,
};

using BufferId = std::uint64_t;
using SegmentId = std::uint64_t;
using BatchId = std::uint64_t;

struct BufferView {
  void* data = nullptr;
  std::size_t bytes = 0;
  DeviceType device = DeviceType::kCPU;
};

struct RegisteredBuffer {
  BufferId buffer_id = 0;
  BufferView view;
  std::string location = "any";
  bool remote_accessible = true;
};

struct RemoteSegmentHandle {
  SegmentId segment_id = 0;
  std::string segment_name;
  // Resolved metadata for transport backend fast path.
  std::string peer_endpoint;
  std::uint64_t remote_base_addr = 0;
};

struct RemoteBufferRef {
  RemoteSegmentHandle segment;
  std::uint64_t offset = 0;
  std::size_t length = 0;
};

enum class TransferOpcode : uint8_t {
  kRead = 0,
  kWrite = 1,
};

enum class TransferState : uint8_t {
  kPending = 0,
  kInFlight = 1,
  kCompleted = 2,
  kFailed = 3,
};

struct TransferStatus {
  TransferState state = TransferState::kPending;
  std::size_t transferred_bytes = 0;
  std::string message;
  int error_code = 0;
};

struct TransferRequest {
  std::uint64_t request_id = 0;
  TransferOpcode opcode = TransferOpcode::kWrite;
  BufferId local_buffer_id = 0;
  RemoteBufferRef remote;
  std::size_t length = 0;
};

struct ResolvedTransferRequest {
  std::uint64_t request_id = 0;
  TransferOpcode opcode = TransferOpcode::kWrite;
  BufferId local_buffer_id = 0;
  BufferView local_view;
  RemoteBufferRef remote;
  std::string peer_endpoint;
  std::uint64_t resolved_remote_addr = 0;
  std::size_t length = 0;
};

struct BatchHandle {
  BatchId batch_id = 0;
  std::size_t capacity = 0;
};

struct TransportCapabilities {
  bool supports_read = true;
  bool supports_write = true;
  bool supports_cuda_buffer = false;
};

}  // namespace nano_mooncake
