#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace nano_mooncake {

enum class DeviceType : uint8_t {
  kCPU = 0,
  kCUDA = 1,
};

struct BufferView {
  void* data = nullptr;
  std::size_t bytes = 0;
  DeviceType device = DeviceType::kCPU;
};

using BufferId = std::uint64_t;
using SegmentId = std::uint64_t;
using BatchId = std::uint64_t;

struct RegisteredBuffer {
  BufferId buffer_id = 0;
  BufferView view;
  std::string location = "any";
  bool remote_accessible = true;
};

struct RemoteSegmentHandle {
  SegmentId segment_id = 0;
  std::string segment_name;
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

struct BatchHandle {
  BatchId batch_id = 0;
  std::size_t capacity = 0;
};

class Engine {
 public:
  Engine() = default;
  ~Engine() = default;

  void init(const std::string& local_addr);
  RegisteredBuffer register_buffer(
      const BufferView& buffer, const std::string& location = "any",
      bool remote_accessible = true);
  RemoteSegmentHandle open_segment(const std::string& segment_name);
  BatchHandle allocate_batch(std::size_t capacity);

  BatchHandle submit_write(BufferId local_buffer_id, const RemoteBufferRef& remote);
  BatchHandle submit_read(const RemoteBufferRef& remote, BufferId local_buffer_id);

  TransferStatus poll(BatchId batch_id) const;
  TransferStatus wait(BatchId batch_id, int timeout_ms = -1) const;
  void close();

 private:
  bool check_overlap(const BufferView& lhs, const BufferView& rhs) const;
  BatchHandle submit_request(const TransferRequest& request);
  std::optional<TransferStatus> get_batch_status(BatchId batch_id) const;

  struct BatchRecord {
    BatchHandle handle;
    std::vector<TransferRequest> requests;
    TransferStatus status;
  };

  std::string local_addr_;
  bool initialized_ = false;
  std::unordered_map<BufferId, RegisteredBuffer> buffer_registry_;
  std::unordered_map<SegmentId, RemoteSegmentHandle> segment_cache_;
  std::unordered_map<std::string, SegmentId> segment_name_to_id_;
  std::unordered_map<BatchId, BatchRecord> batch_table_;
  std::unordered_set<void*> raw_addr_registry_;
  BufferId next_buffer_id_ = 1;
  SegmentId next_segment_id_ = 1;
  BatchId next_batch_id_ = 1;
  std::uint64_t next_request_id_ = 1;
};

}  // namespace nano_mooncake
