#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transport.h"
#include "types.h"

namespace nano_mooncake {

class Engine {
 public:
  Engine() = default;
  ~Engine() = default;

  // Preferred lifecycle API (aligned with TransportBackend start/stop).
  void start(const std::string& local_addr);
  void stop();

  // Backward-compatible aliases.
  void init(const std::string& local_addr);
  RegisteredBuffer register_buffer(
      const BufferView& buffer, const std::string& location = "any",
      bool remote_accessible = true);
  void unregister_buffer(BufferId buffer_id);
  RemoteSegmentHandle open_segment(const std::string& segment_name);
  void close_segment(SegmentId segment_id);
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
  std::unique_ptr<TransportBackend> transportbackend;
  BufferId next_buffer_id_ = 1;
  SegmentId next_segment_id_ = 1;
  BatchId next_batch_id_ = 1;
  std::uint64_t next_request_id_ = 1;
};

}  // namespace nano_mooncake
