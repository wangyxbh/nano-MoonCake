#pragma once

#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace nano_mooncake {

// Reusable transport skeleton shared by TCP/RDMA/etc.
// Subclasses only implement endpoint-specific lifecycle and request execution.
class Transport {
 public:
  virtual ~Transport() = default;

  virtual std::string name() const = 0;
  virtual TransportCapabilities capabilities() const = 0;

  void start(const std::string& local_endpoint);
  void stop();

  void add_local_buffer(const RegisteredBuffer& buffer);
  void remove_local_buffer(BufferId buffer_id);
  void add_local_segment(const MountedSegment& segment);
  void remove_local_segment(const std::string& segment_name);

  void prepare_segment(RemoteSegmentHandle& segment);
  virtual void release_segment(SegmentId segment_id) {}

  void submit(
      BatchId batch_id,
      const std::vector<ResolvedTransferRequest>& requests);

  TransferStatus poll(BatchId batch_id, std::size_t task_id = 0);

 protected:
  virtual void do_start(const std::string& local_endpoint) = 0;
  virtual void do_stop() = 0;
  virtual void do_prepare_segment(RemoteSegmentHandle& segment) {}
  virtual void do_submit_request(const ResolvedTransferRequest& request) = 0;

  void set_batch_status(BatchId batch_id, const TransferStatus& status);
  MountedSegment get_exported_segment(const std::string& segment_name) const;
  RegisteredBuffer get_registered_buffer(BufferId buffer_id) const;

 private:
  mutable std::mutex buffers_mu_;
  mutable std::mutex segments_mu_;
  mutable std::mutex batches_mu_;
  std::unordered_map<BufferId, RegisteredBuffer> local_buffers_;
  std::unordered_map<std::string, MountedSegment> local_segments_;
  std::unordered_map<BatchId, TransferStatus> batch_status_;
  bool started_ = false;
};

using TransportBackend = Transport;

}  // namespace nano_mooncake
