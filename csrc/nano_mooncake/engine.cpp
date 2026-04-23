#include "nano_mooncake/engine.h"

#include <chrono>
#include <thread>
#include <stdexcept>

namespace nano_mooncake {

namespace {
constexpr int kTimeoutError = 1;
}

void Engine::start(const std::string& local_addr) { init(local_addr); }

void Engine::stop() { close(); }

void Engine::init(const std::string& local_addr) {
  if (local_addr.empty()) {
    throw std::invalid_argument("local_addr must not be empty");
  }
  if (transportbackend) {
    transportbackend->start(local_addr);
  }
  local_addr_ = local_addr;
  initialized_ = true;
}

RegisteredBuffer Engine::register_buffer(const BufferView& buffer,
                                         const std::string& location,
                                         bool remote_accessible) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (buffer.data == nullptr || buffer.bytes == 0) {
    throw std::invalid_argument("buffer must not be empty");
  }
  for (const auto& [_, existing] : buffer_registry_) {
    if (check_overlap(buffer, existing.view)) {
      throw std::invalid_argument("buffer overlaps with an existing registration");
    }
  }
  if (raw_addr_registry_.count(buffer.data) > 0) {
    throw std::invalid_argument("buffer address already registered");
  }

  RegisteredBuffer reg{
      .buffer_id = next_buffer_id_++,
      .view = buffer,
      .location = location,
      .remote_accessible = remote_accessible,
  };
  raw_addr_registry_.insert(buffer.data);
  buffer_registry_.emplace(reg.buffer_id, reg);
  if (transportbackend) {
    transportbackend->add_local_buffer(reg);
  }
  return reg;
}

void Engine::unregister_buffer(BufferId buffer_id) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto it = buffer_registry_.find(buffer_id);
  if (it == buffer_registry_.end()) {
    throw std::invalid_argument("buffer_id not found");
  }
  if (transportbackend) {
    transportbackend->remove_local_buffer(buffer_id);
  }
  raw_addr_registry_.erase(it->second.view.data);
  buffer_registry_.erase(it);
}

RemoteSegmentHandle Engine::open_segment(const std::string& segment_name) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }
  if (auto it = segment_name_to_id_.find(segment_name);
      it != segment_name_to_id_.end()) {
    return segment_cache_.at(it->second);
  }
  RemoteSegmentHandle handle{
      .segment_id = next_segment_id_++,
      .segment_name = segment_name,
      .peer_endpoint = segment_name,
      .remote_base_addr = 0,
  };
  segment_name_to_id_[segment_name] = handle.segment_id;
  segment_cache_[handle.segment_id] = handle;
  if (transportbackend) {
    transportbackend->prepare_segment(handle);
  }
  return handle;
}

void Engine::close_segment(SegmentId segment_id) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto segment_it = segment_cache_.find(segment_id);
  if (segment_it == segment_cache_.end()) {
    throw std::invalid_argument("segment_id not found");
  }
  if (transportbackend) {
    transportbackend->release_segment(segment_id);
  }
  segment_name_to_id_.erase(segment_it->second.segment_name);
  segment_cache_.erase(segment_it);
}

BatchHandle Engine::allocate_batch(std::size_t capacity) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (capacity == 0) {
    throw std::invalid_argument("capacity must be greater than 0");
  }
  BatchHandle handle{.batch_id = next_batch_id_++, .capacity = capacity};
  BatchRecord record{
      .handle = handle,
      .requests = {},
      .status =
          TransferStatus{
              .state = TransferState::kPending,
              .transferred_bytes = 0,
              .message = "batch allocated",
              .error_code = 0,
          },
  };
  record.requests.reserve(capacity);
  batch_table_.emplace(handle.batch_id, std::move(record));
  return handle;
}

BatchHandle Engine::submit_write(BufferId local_buffer_id,
                                 const RemoteBufferRef& remote) {
  TransferRequest request{
      .request_id = next_request_id_++,
      .opcode = TransferOpcode::kWrite,
      .local_buffer_id = local_buffer_id,
      .remote = remote,
      .length = remote.length,
  };
  return submit_request(request);
}

BatchHandle Engine::submit_read(const RemoteBufferRef& remote,
                                BufferId local_buffer_id) {
  TransferRequest request{
      .request_id = next_request_id_++,
      .opcode = TransferOpcode::kRead,
      .local_buffer_id = local_buffer_id,
      .remote = remote,
      .length = remote.length,
  };
  return submit_request(request);
}

TransferStatus Engine::poll(BatchId batch_id) const {
  auto status = get_batch_status(batch_id);
  if (!status.has_value()) {
    throw std::invalid_argument("batch_id not found");
  }
  return status.value();
}

TransferStatus Engine::wait(BatchId batch_id, int timeout_ms) const {
  auto start = std::chrono::steady_clock::now();
  while (true) {
    auto status = poll(batch_id);
    if (status.state == TransferState::kCompleted ||
        status.state == TransferState::kFailed) {
      return status;
    }
    if (timeout_ms >= 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      if (elapsed > timeout_ms) {
        return TransferStatus{
            .state = TransferState::kFailed,
            .transferred_bytes = status.transferred_bytes,
            .message = "wait timeout",
            .error_code = kTimeoutError,
        };
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void Engine::close() {
  if (!initialized_) {
    return;
  }
  std::vector<SegmentId> segments_to_close;
  segments_to_close.reserve(segment_cache_.size());
  for (const auto& [segment_id, _] : segment_cache_) {
    segments_to_close.push_back(segment_id);
  }
  for (auto segment_id : segments_to_close) {
    close_segment(segment_id);
  }

  std::vector<BufferId> buffers_to_unregister;
  buffers_to_unregister.reserve(buffer_registry_.size());
  for (const auto& [buffer_id, _] : buffer_registry_) {
    buffers_to_unregister.push_back(buffer_id);
  }
  for (auto buffer_id : buffers_to_unregister) {
    unregister_buffer(buffer_id);
  }

  batch_table_.clear();
  if (transportbackend) {
    transportbackend->stop();
  }
  initialized_ = false;
  local_addr_.clear();
}

bool Engine::check_overlap(const BufferView& lhs, const BufferView& rhs) const {
  auto* lhs_begin = static_cast<std::uint8_t*>(lhs.data);
  auto* lhs_end = lhs_begin + lhs.bytes;
  auto* rhs_begin = static_cast<std::uint8_t*>(rhs.data);
  auto* rhs_end = rhs_begin + rhs.bytes;
  return lhs_begin < rhs_end && rhs_begin < lhs_end;
}

BatchHandle Engine::submit_request(const TransferRequest& request) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto local_it = buffer_registry_.find(request.local_buffer_id);
  if (local_it == buffer_registry_.end()) {
    throw std::invalid_argument("local buffer is not registered");
  }
  if (request.remote.length == 0 || request.length == 0) {
    throw std::invalid_argument("request length must be greater than 0");
  }
  if (request.remote.length != request.length) {
    throw std::invalid_argument("request length mismatch");
  }
  if (request.length > local_it->second.view.bytes) {
    throw std::invalid_argument("request exceeds local buffer length");
  }
  auto segment_it = segment_cache_.find(request.remote.segment.segment_id);
  if (segment_it == segment_cache_.end()) {
    throw std::invalid_argument("remote segment is not opened");
  }

  ResolvedTransferRequest resolved{
      .request_id = request.request_id,
      .opcode = request.opcode,
      .local_buffer_id = request.local_buffer_id,
      .local_view = local_it->second.view,
      .remote = request.remote,
      .peer_endpoint = segment_it->second.peer_endpoint,
      .resolved_remote_addr =
          segment_it->second.remote_base_addr + request.remote.offset,
      .length = request.length,
  };

  auto handle = allocate_batch(1);
  auto& record = batch_table_.at(handle.batch_id);
  record.status.state = TransferState::kInFlight;
  record.status.message = "request submitted";
  record.requests.push_back(request);

  if (transportbackend) {
    transportbackend->submit(handle.batch_id, {resolved});
  } else {
    record.status.state = TransferState::kCompleted;
    record.status.transferred_bytes = request.length;
    record.status.message = "completed by stage-0 stub";
  }
  return handle;
}



std::optional<TransferStatus> Engine::get_batch_status(BatchId batch_id) const {
  auto it = batch_table_.find(batch_id);
  if (it == batch_table_.end()) {
    return std::nullopt;
  }
  return it->second.status;
}

}  // namespace nano_mooncake
