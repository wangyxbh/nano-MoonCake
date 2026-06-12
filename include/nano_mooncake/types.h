#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace nano_mooncake {

enum class DeviceType : uint8_t {
  kCPU = 0,
  kCUDA = 1,
};

enum class TransportKind : uint8_t {
  kTcp = 0,
  kRdma = 1,
};

inline const char* TransportKindName(TransportKind kind) {
  switch (kind) {
    case TransportKind::kTcp:
      return "tcp";
    case TransportKind::kRdma:
      return "rdma";
  }
  return "unknown";
}

inline TransportKind InferTransportKind(std::string_view endpoint) {
  if (endpoint.rfind("tcp://", 0) == 0) {
    return TransportKind::kTcp;
  }
  if (endpoint.rfind("rdma://", 0) == 0) {
    return TransportKind::kRdma;
  }
  throw std::invalid_argument("unsupported transport endpoint scheme");
}

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

struct MountedSegment {
  std::string segment_name;
  BufferId buffer_id = 0;
  std::string transport_endpoint;
  std::uint64_t base_offset = 0;
  std::size_t bytes = 0;
};

enum class SegmentStatus : uint8_t {
  kUndefined = 0,
  kOk = 1,
  kUnmounted = 2,
};

struct MasterSegmentRecord {
  std::string segment_name;
  std::string transport_endpoint;
  std::uint64_t base_offset = 0;
  std::size_t bytes = 0;
  SegmentStatus status = SegmentStatus::kOk;
  std::string owner_client_id;
};

struct ReplicaLocation {
  std::string segment_name;
  std::string transport_endpoint;
  std::uint64_t offset = 0;
  std::size_t length = 0;
  std::string owner_client_id;
};

struct ObjectLocationRecord {
  std::string key;
  std::string segment_name;
  std::string transport_endpoint;
  std::uint64_t offset = 0;
  std::size_t length = 0;
  std::string owner_client_id;
  std::vector<ReplicaLocation> replicas;
};

struct RemoteSegmentHandle {
  SegmentId segment_id = 0;
  std::string segment_name;
  std::string peer_endpoint;
  std::uint64_t remote_base_addr = 0;
  std::size_t remote_bytes = 0;
  std::uint32_t remote_key = 0;
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
