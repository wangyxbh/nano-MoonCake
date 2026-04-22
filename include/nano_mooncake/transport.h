#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "types.h"

namespace nano_mooncake{

class TransportBackend {
 public:
  virtual ~TransportBackend() = default;
  virtual std::string name() const = 0;
  virtual TransportCapabilities capabilities() const = 0;

  virtual void start(const std::string& local_endpoint) = 0;
  virtual void stop() = 0;

  virtual void add_local_buffer(const RegisteredBuffer& buffer) = 0;
  virtual void remove_local_buffer(BufferId buffer_id) = 0;

  virtual void prepare_segment(const RemoteSegmentHandle& segment) {}
  virtual void release_segment(SegmentId segment_id) {}

  virtual void submit(
      BatchId batch_id,
      const std::vector<ResolvedTransferRequest>& requests) = 0;

  virtual TransferStatus poll(BatchId batch_id, std::size_t task_id = 0) = 0;
};

}  // namespace nano_mooncake
