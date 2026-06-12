#include "nano_mooncake/engine.h"

#include <chrono>
#include <stdexcept>
#include <thread>

#include "nano_mooncake/observability.h"
#ifdef NANO_HAS_RDMA
#include "nano_mooncake/rdma_transport.h"
#endif
#include "nano_mooncake/tcp_transport.h"

namespace nano_mooncake {

namespace {
constexpr int kTimeoutError = 1;

std::unique_ptr<TransportBackend> CreateTransportBackend(
    const std::string& local_addr) {
  switch (InferTransportKind(local_addr)) {
    case TransportKind::kTcp:
      return std::make_unique<TcpTransportBackend>();
    case TransportKind::kRdma:
#ifdef NANO_HAS_RDMA
      return std::make_unique<RdmaTransportBackend>();
#else
      throw std::runtime_error(
          "RDMA endpoint requested, but nano-MoonCake was built without RDMA support");
#endif
  }
  throw std::invalid_argument("unsupported transport backend");
}
}

Engine::Engine() = default;

void Engine::start(const std::string& local_addr, const std::string& master_addr,
                   const std::string& client_id) {
  init(local_addr, master_addr, client_id);
}

void Engine::stop() { close(); }

void Engine::init(const std::string& local_addr, const std::string& master_addr,
                  const std::string& client_id) {
  if (local_addr.empty()) {
    throw std::invalid_argument("local_addr must not be empty");
  }
  if (master_addr.empty()) {
    throw std::invalid_argument("master_addr must not be empty");
  }
  if (client_id.empty()) {
    throw std::invalid_argument("client_id must not be empty");
  }
  transportbackend = CreateTransportBackend(local_addr);
  if (transportbackend) {
    transportbackend->start(local_addr);
  }
  local_addr_ = local_addr;
  master_addr_ = master_addr;
  client_id_ = client_id;
  master_client_ = std::make_unique<MasterClient>(master_addr_);
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
      throw std::invalid_argument(
          "buffer overlaps with an existing registration");
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
  for (const auto& [segment_name, segment] : mounted_segments_) {
    if (segment.buffer_id == buffer_id) {
      throw std::invalid_argument(
          "buffer is still mounted as segment " + segment_name);
    }
  }
  if (transportbackend) {
    transportbackend->remove_local_buffer(buffer_id);
  }
  raw_addr_registry_.erase(it->second.view.data);
  buffer_registry_.erase(it);
}

MountedSegment Engine::mount_segment(const std::string& segment_name,
                                     BufferId buffer_id,
                                     const std::string& transport_endpoint) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }
  auto buffer_it = buffer_registry_.find(buffer_id);
  if (buffer_it == buffer_registry_.end()) {
    throw std::invalid_argument("buffer_id not found");
  }
  if (!buffer_it->second.remote_accessible) {
    throw std::invalid_argument("buffer is not remote accessible");
  }
  if (mounted_segments_.find(segment_name) != mounted_segments_.end()) {
    throw std::invalid_argument("segment_name already mounted locally");
  }

  MountedSegment mounted{
      .segment_name = segment_name,
      .buffer_id = buffer_id,
      .transport_endpoint =
          transport_endpoint.empty() ? local_addr_ : transport_endpoint,
      .base_offset = 0,
      .bytes = buffer_it->second.view.bytes,
  };
  mounted_segments_.emplace(segment_name, mounted);
  if (transportbackend) {
    transportbackend->add_local_segment(mounted);
  }
  master_client_->MountSegment(MasterSegmentRecord{
      .segment_name = mounted.segment_name,
      .transport_endpoint = mounted.transport_endpoint,
      .base_offset = mounted.base_offset,
      .bytes = mounted.bytes,
      .status = SegmentStatus::kOk,
      .owner_client_id = client_id_,
  });
  return mounted;
}

void Engine::unmount_segment(const std::string& segment_name) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto it = mounted_segments_.find(segment_name);
  if (it == mounted_segments_.end()) {
    throw std::invalid_argument("segment_name is not mounted locally");
  }
  if (transportbackend) {
    transportbackend->remove_local_segment(segment_name);
  }
  master_client_->UnmountSegment(segment_name);
  mounted_segments_.erase(it);
}

void Engine::unsegment(const std::string& segment_name) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }

  bool closed_remote = false;
  if (auto it = segment_name_to_id_.find(segment_name);
      it != segment_name_to_id_.end()) {
    close_segment(it->second);
    closed_remote = true;
  }

  bool unmounted_local = false;
  if (mounted_segments_.find(segment_name) != mounted_segments_.end()) {
    unmount_segment(segment_name);
    unmounted_local = true;
  }

  if (!closed_remote && !unmounted_local) {
    throw std::invalid_argument(
        "segment_name is neither opened nor mounted locally");
  }
}

std::optional<MasterSegmentRecord> Engine::resolve_segment(
    const std::string& segment_name) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  return master_client_->ResolveSegment(segment_name);
}

void Engine::put_object(const std::string& key, const std::string& segment_name,
                        std::uint64_t offset, std::size_t length) {
  RootTraceScope root_trace;
  const auto start = TraceNow();
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto segment_it = mounted_segments_.find(segment_name);
  if (segment_it == mounted_segments_.end()) {
    throw std::invalid_argument("segment_name is not mounted locally");
  }
  if (length == 0) {
    throw std::invalid_argument("object length must be greater than 0");
  }
  if (offset > segment_it->second.bytes ||
      length > segment_it->second.bytes - offset) {
    throw std::invalid_argument("object range exceeds mounted segment bounds");
  }
  ObjectLocationRecord object{
      .key = key,
      .segment_name = segment_name,
      .transport_endpoint = segment_it->second.transport_endpoint,
      .offset = offset,
      .length = length,
      .owner_client_id = client_id_,
      .replicas =
          {ReplicaLocation{
              .segment_name = segment_name,
              .transport_endpoint = segment_it->second.transport_endpoint,
              .offset = offset,
              .length = length,
              .owner_client_id = client_id_,
          }},
  };
  master_client_->PutObject(object);
  LogTrace("engine", "put_object",
           TraceFields{
               .bytes = length,
               .offset = offset,
               .duration_us = ElapsedUs(start, TraceNow()),
               .segment_name = segment_name,
               .object_key = key,
               .status = "ok",
           });
}

std::optional<ObjectLocationRecord> Engine::get_object(const std::string& key) {
  RootTraceScope root_trace;
  const auto start = TraceNow();
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  auto object = master_client_->GetObject(key);
  LogTrace("engine", "get_object",
           TraceFields{
               .bytes = object.has_value() ? object->length : 0,
               .duration_us = ElapsedUs(start, TraceNow()),
               .segment_name = object.has_value() ? object->segment_name : "",
               .object_key = key,
               .status = object.has_value() ? "ok" : "miss",
           });
  return object;
}

RemoteSegmentHandle Engine::open_segment(const std::string& segment_name) {
  RootTraceScope root_trace;
  const auto start = TraceNow();
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }
  if (auto it = segment_name_to_id_.find(segment_name);
      it != segment_name_to_id_.end()) {
    LogTrace("engine", "open_segment",
             TraceFields{
                 .segment_id = it->second,
                 .duration_us = ElapsedUs(start, TraceNow()),
                 .segment_name = segment_name,
                 .status = "ok",
                 .cache_hit = true,
                 .has_cache_hit = true,
             });
    return segment_cache_.at(it->second);
  }
  auto record = master_client_->ResolveSegment(segment_name);
  if (!record.has_value()) {
    throw std::invalid_argument("segment_name is not registered in master");
  }
  RemoteSegmentHandle handle{
      .segment_id = next_segment_id_++,
      .segment_name = record->segment_name,
      .peer_endpoint = record->transport_endpoint,
      .remote_base_addr = record->base_offset,
      .remote_bytes = record->bytes,
  };
  if (transportbackend) {
    transportbackend->prepare_segment(handle);
  }
  segment_name_to_id_[segment_name] = handle.segment_id;
  segment_cache_[handle.segment_id] = handle;
  LogTrace("engine", "open_segment",
           TraceFields{
               .segment_id = handle.segment_id,
               .bytes = handle.remote_bytes,
               .duration_us = ElapsedUs(start, TraceNow()),
               .segment_name = handle.segment_name,
               .endpoint = handle.peer_endpoint,
               .status = "ok",
               .cache_hit = false,
               .has_cache_hit = true,
           });
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
      .trace_id = CurrentTraceId(),
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

BatchHandle Engine::read_object(const std::string& key,
                                BufferId local_buffer_id) {
  RootTraceScope root_trace;
  const auto start = TraceNow();
  auto object = get_object(key);
  if (!object.has_value()) {
    throw std::invalid_argument("object key not found");
  }
  auto segment = open_segment(object->segment_name);
  RemoteBufferRef remote{
      .segment = segment,
      .offset = object->offset,
      .length = object->length,
  };
  auto handle = submit_read(remote, local_buffer_id);
  LogTrace("engine", "read_object",
           TraceFields{
               .batch_id = handle.batch_id,
               .segment_id = segment.segment_id,
               .offset = object->offset,
               .bytes = object->length,
               .duration_us = ElapsedUs(start, TraceNow()),
               .segment_name = object->segment_name,
               .object_key = key,
               .status = "ok",
           });
  return handle;
}

TransferStatus Engine::poll(BatchId batch_id) {
  auto it = batch_table_.find(batch_id);
  if (it == batch_table_.end()) {
    throw std::invalid_argument("batch_id not found");
  }
  if (transportbackend) {
    it->second.status = transportbackend->poll(batch_id);
  }
  return it->second.status;
}

TransferStatus Engine::wait(BatchId batch_id, int timeout_ms) {
  RootTraceScope root_trace;
  auto record_it = batch_table_.find(batch_id);
  if (record_it == batch_table_.end()) {
    throw std::invalid_argument("batch_id not found");
  }
  TraceContextScope trace_scope(record_it->second.trace_id);
  auto start = std::chrono::steady_clock::now();
  while (true) {
    auto status = poll(batch_id);
    if (status.state == TransferState::kCompleted ||
        status.state == TransferState::kFailed) {
      LogTrace("engine", "wait_batch",
               TraceFields{
                   .batch_id = batch_id,
                   .bytes = status.transferred_bytes,
                   .duration_us = static_cast<std::uint64_t>(
                       std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count()),
                   .status = status.state == TransferState::kCompleted ? "ok"
                                                                        : "error",
                   .error_code = status.error_code,
                   .message = status.message,
               });
      return status;
    }
    if (timeout_ms >= 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      if (elapsed > timeout_ms) {
        LogTrace("engine", "wait_batch",
                 TraceFields{
                     .batch_id = batch_id,
                     .bytes = status.transferred_bytes,
                     .duration_us = static_cast<std::uint64_t>(elapsed) * 1000,
                     .status = "timeout",
                     .error_code = kTimeoutError,
                     .message = "wait timeout",
                 });
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

  std::vector<std::string> mounted_names;
  mounted_names.reserve(mounted_segments_.size());
  for (const auto& [segment_name, _] : mounted_segments_) {
    mounted_names.push_back(segment_name);
  }
  for (const auto& segment_name : mounted_names) {
    unmount_segment(segment_name);
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
  master_client_.reset();
  if (transportbackend) {
    transportbackend->stop();
  }
  initialized_ = false;
  local_addr_.clear();
  master_addr_.clear();
  client_id_.clear();
}

bool Engine::check_overlap(const BufferView& lhs, const BufferView& rhs) const {
  auto* lhs_begin = static_cast<std::uint8_t*>(lhs.data);
  auto* lhs_end = lhs_begin + lhs.bytes;
  auto* rhs_begin = static_cast<std::uint8_t*>(rhs.data);
  auto* rhs_end = rhs_begin + rhs.bytes;
  return lhs_begin < rhs_end && rhs_begin < lhs_end;
}

BatchHandle Engine::submit_request(const TransferRequest& request) {
  RootTraceScope root_trace;
  const auto start = TraceNow();
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
  if (segment_it->second.remote_bytes > 0 &&
      (request.remote.offset > segment_it->second.remote_bytes ||
       request.length > segment_it->second.remote_bytes - request.remote.offset)) {
    throw std::invalid_argument("request exceeds remote segment length");
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
  record.trace_id = CurrentTraceId();
  record.status.state = TransferState::kInFlight;
  record.status.message = "request submitted";
  record.requests.push_back(request);

  if (transportbackend) {
    transportbackend->submit(handle.batch_id, {resolved});
  }
  record.status = poll(handle.batch_id);
  LogTrace("engine", "submit_request",
           TraceFields{
               .request_id = request.request_id,
               .batch_id = handle.batch_id,
               .segment_id = request.remote.segment.segment_id,
               .offset = request.remote.offset,
               .bytes = request.length,
               .duration_us = ElapsedUs(start, TraceNow()),
               .opcode = TransferOpcodeName(
                   static_cast<std::uint8_t>(request.opcode)),
               .segment_name = request.remote.segment.segment_name,
               .endpoint = resolved.peer_endpoint,
               .status =
                   record.status.state == TransferState::kCompleted ? "ok" : "error",
               .error_code = record.status.error_code,
               .message = record.status.message,
           });
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
