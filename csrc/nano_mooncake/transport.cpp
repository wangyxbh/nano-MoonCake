#include "nano_mooncake/transport.h"

#include <cerrno>
#include <stdexcept>

#include "nano_mooncake/observability.h"

namespace nano_mooncake {

void Transport::start(const std::string& local_endpoint) {
  if (started_) {
    throw std::runtime_error("transport already started");
  }
  do_start(local_endpoint);
  started_ = true;
}

void Transport::stop() {
  if (!started_) {
    return;
  }
  do_stop();
  {
    std::lock_guard<std::mutex> lock(buffers_mu_);
    local_buffers_.clear();
  }
  {
    std::lock_guard<std::mutex> lock(segments_mu_);
    local_segments_.clear();
  }
  {
    std::lock_guard<std::mutex> lock(batches_mu_);
    batch_status_.clear();
  }
  started_ = false;
}

void Transport::add_local_buffer(const RegisteredBuffer& buffer) {
  {
    std::lock_guard<std::mutex> lock(buffers_mu_);
    local_buffers_[buffer.buffer_id] = buffer;
  }
  try {
    do_add_local_buffer(buffer);
  } catch (...) {
    std::lock_guard<std::mutex> lock(buffers_mu_);
    local_buffers_.erase(buffer.buffer_id);
    throw;
  }
}

void Transport::remove_local_buffer(BufferId buffer_id) {
  RegisteredBuffer removed;
  {
    std::lock_guard<std::mutex> lock(buffers_mu_);
    auto it = local_buffers_.find(buffer_id);
    if (it == local_buffers_.end()) {
      return;
    }
    removed = it->second;
    local_buffers_.erase(it);
  }
  do_remove_local_buffer(removed);
}

void Transport::add_local_segment(const MountedSegment& segment) {
  {
    std::lock_guard<std::mutex> lock(segments_mu_);
    local_segments_[segment.segment_name] = segment;
  }
  try {
    do_add_local_segment(segment);
  } catch (...) {
    std::lock_guard<std::mutex> lock(segments_mu_);
    local_segments_.erase(segment.segment_name);
    throw;
  }
}

void Transport::remove_local_segment(const std::string& segment_name) {
  MountedSegment removed;
  {
    std::lock_guard<std::mutex> lock(segments_mu_);
    auto it = local_segments_.find(segment_name);
    if (it == local_segments_.end()) {
      return;
    }
    removed = it->second;
    local_segments_.erase(it);
  }
  do_remove_local_segment(removed);
}

void Transport::prepare_segment(RemoteSegmentHandle& segment) {
  do_prepare_segment(segment);
}

void Transport::submit(
    BatchId batch_id,
    const std::vector<ResolvedTransferRequest>& requests) {
  const auto submit_start = TraceNow();
  set_batch_status(
      batch_id,
      TransferStatus{
          .state = TransferState::kInFlight,
          .transferred_bytes = 0,
          .message = name() + " submit in progress",
          .error_code = 0,
      });

  try {
    std::size_t transferred = 0;
    for (const auto& request : requests) {
      do_submit_request(request);
      transferred += request.length;
    }
    set_batch_status(
        batch_id,
        TransferStatus{
            .state = TransferState::kCompleted,
            .transferred_bytes = transferred,
            .message = name() + " transfer completed",
            .error_code = 0,
        });
    LogTrace("transport", "submit_batch",
             TraceFields{
                 .batch_id = batch_id,
                 .bytes = transferred,
                 .duration_us = ElapsedUs(submit_start, TraceNow()),
                 .item_count = requests.size(),
                 .status = "ok",
                 .message = name(),
             });
  } catch (const std::exception& ex) {
    set_batch_status(
        batch_id,
        TransferStatus{
            .state = TransferState::kFailed,
            .transferred_bytes = 0,
            .message = ex.what(),
            .error_code = errno,
        });
    LogTrace("transport", "submit_batch",
             TraceFields{
                 .batch_id = batch_id,
                 .duration_us = ElapsedUs(submit_start, TraceNow()),
                 .item_count = requests.size(),
                 .error_code = errno,
                 .status = "error",
                 .message = ex.what(),
             });
  }
}

TransferStatus Transport::poll(BatchId batch_id, std::size_t /*task_id*/) {
  std::lock_guard<std::mutex> lock(batches_mu_);
  auto it = batch_status_.find(batch_id);
  if (it == batch_status_.end()) {
    throw std::invalid_argument("batch_id not found in transport");
  }
  return it->second;
}

void Transport::set_batch_status(BatchId batch_id, const TransferStatus& status) {
  std::lock_guard<std::mutex> lock(batches_mu_);
  batch_status_[batch_id] = status;
}

MountedSegment Transport::get_exported_segment(
    const std::string& segment_name) const {
  std::lock_guard<std::mutex> lock(segments_mu_);
  auto it = local_segments_.find(segment_name);
  if (it == local_segments_.end()) {
    throw std::runtime_error("segment is not mounted on this peer");
  }
  return it->second;
}

RegisteredBuffer Transport::get_registered_buffer(BufferId buffer_id) const {
  std::lock_guard<std::mutex> lock(buffers_mu_);
  auto it = local_buffers_.find(buffer_id);
  if (it == local_buffers_.end()) {
    throw std::runtime_error("mounted segment buffer is not registered");
  }
  return it->second;
}

}  // namespace nano_mooncake
