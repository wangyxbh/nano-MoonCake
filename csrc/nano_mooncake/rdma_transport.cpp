#include "nano_mooncake/rdma_transport.h"

#ifdef NANO_HAS_RDMA

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef NANO_HAS_CUDA
#include <cuda.h>
#endif

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

#include "nano_mooncake/observability.h"

namespace nano_mooncake {

namespace {

constexpr std::uint32_t kHandshakeMagic = 0x4e4d5244;  // "NMRD"
constexpr std::uint16_t kHandshakeVersion = 1;
constexpr std::uint32_t kStatusOk = 0;
constexpr std::uint32_t kStatusError = 1;
constexpr int kDefaultPortNum = 1;
constexpr int kBaseAccessFlags =
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

struct QueuePairWire {
  std::uint32_t qp_num = 0;
  std::uint32_t psn = 0;
  std::uint16_t lid = 0;
  std::uint8_t gid_index = 0;
  std::uint8_t use_global = 0;
  std::uint8_t reserved[2] = {};
  std::uint8_t gid[16] = {};
};

struct PrepareHeaderWire {
  std::uint32_t magic = 0;
  std::uint16_t version = 0;
  std::uint16_t name_length = 0;
  std::uint64_t trace_id = 0;
  QueuePairWire initiator = {};
};

struct PrepareReplyWire {
  std::uint32_t status = 0;
  std::uint32_t remote_key = 0;
  std::uint64_t remote_base_addr = 0;
  std::uint64_t remote_bytes = 0;
  QueuePairWire responder = {};
  char message[96] = {};
};

struct ParsedEndpoint {
  std::string host;
  std::uint16_t port = 0;
  std::string device_name;
  int port_num = kDefaultPortNum;
  int gid_index = 0;
};

std::unordered_map<std::string, std::string> ParseQueryString(
    const std::string& query) {
  std::unordered_map<std::string, std::string> params;
  std::size_t start = 0;
  while (start < query.size()) {
    auto end = query.find('&', start);
    if (end == std::string::npos) {
      end = query.size();
    }
    auto token = query.substr(start, end - start);
    auto eq = token.find('=');
    if (!token.empty()) {
      if (eq == std::string::npos) {
        params[token] = "";
      } else {
        params[token.substr(0, eq)] = token.substr(eq + 1);
      }
    }
    start = end + 1;
  }
  return params;
}

ParsedEndpoint ParseEndpoint(const std::string& endpoint) {
  constexpr const char kPrefix[] = "rdma://";
  if (endpoint.rfind(kPrefix, 0) != 0) {
    throw std::invalid_argument("RDMA endpoint must use rdma:// scheme");
  }

  auto body = endpoint.substr(sizeof(kPrefix) - 1);
  std::string authority = body;
  std::string query;
  auto query_pos = body.find('?');
  if (query_pos != std::string::npos) {
    authority = body.substr(0, query_pos);
    query = body.substr(query_pos + 1);
  }

  auto colon = authority.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= authority.size()) {
    throw std::invalid_argument("RDMA endpoint must be in rdma://host:port format");
  }

  ParsedEndpoint parsed;
  parsed.host = authority.substr(0, colon);
  int port = std::stoi(authority.substr(colon + 1));
  if (port <= 0 || port > 65535) {
    throw std::invalid_argument("RDMA endpoint port is out of range");
  }
  parsed.port = static_cast<std::uint16_t>(port);

  auto params = ParseQueryString(query);
  auto it = params.find("device");
  if (it != params.end()) {
    parsed.device_name = it->second;
  }
  it = params.find("port_num");
  if (it != params.end() && !it->second.empty()) {
    parsed.port_num = std::stoi(it->second);
  }
  it = params.find("gid_index");
  if (it != params.end() && !it->second.empty()) {
    parsed.gid_index = std::stoi(it->second);
  }
  return parsed;
}

int ConnectTo(const ParsedEndpoint& endpoint) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create RDMA handshake socket");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(endpoint.port);
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::invalid_argument("RDMA peer host must be an IPv4 address");
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    throw std::runtime_error("failed to connect to RDMA handshake peer");
  }
  return fd;
}

void ReadFully(int fd, void* data, std::size_t bytes) {
  auto* cursor = static_cast<std::uint8_t*>(data);
  while (bytes > 0) {
    auto received = ::recv(fd, cursor, bytes, 0);
    if (received <= 0) {
      throw std::runtime_error("RDMA handshake socket read failed");
    }
    cursor += received;
    bytes -= static_cast<std::size_t>(received);
  }
}

void WriteFully(int fd, const void* data, std::size_t bytes) {
  auto* cursor = static_cast<const std::uint8_t*>(data);
  while (bytes > 0) {
    auto written = ::send(fd, cursor, bytes, 0);
    if (written <= 0) {
      throw std::runtime_error("RDMA handshake socket write failed");
    }
    cursor += written;
    bytes -= static_cast<std::size_t>(written);
  }
}

std::string TrimMessage(const char* raw, std::size_t bytes) {
  std::size_t length = 0;
  while (length < bytes && raw[length] != '\0') {
    ++length;
  }
  return std::string(raw, length);
}

std::uint32_t RandomPsn() {
  thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<std::uint32_t> dist(0, 0x00ffffffu);
  return dist(rng);
}

bool GidIsZero(const ibv_gid& gid) {
  for (auto byte : gid.raw) {
    if (byte != 0) {
      return false;
    }
  }
  return true;
}

std::string CompletionError(ibv_wc_status status) {
  const char* raw = ibv_wc_status_str(status);
  return raw == nullptr ? "unknown RDMA completion error" : std::string(raw);
}

bool GetBoolEnv(const char* name, bool default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return default_value;
  }
  std::string value(raw);
  for (auto& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return value == "1" || value == "true" || value == "on" || value == "yes";
}

void EnsureSuccess(int rc, const std::string& message) {
  if (rc != 0) {
    throw std::runtime_error(message);
  }
}

}  // namespace

struct RdmaTransportBackend::Impl {
  struct LocalMemoryRegion {
    ibv_mr* mr = nullptr;
    void* addr = nullptr;
    std::size_t bytes = 0;
    DeviceType device = DeviceType::kCPU;
  };

  struct Connection {
    std::string segment_name;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    QueuePairWire local = {};
    QueuePairWire remote = {};
    std::uint32_t remote_key = 0;

    ~Connection() {
      if (qp != nullptr) {
        ibv_destroy_qp(qp);
        qp = nullptr;
      }
      if (cq != nullptr) {
        ibv_destroy_cq(cq);
        cq = nullptr;
      }
    }
  };

  ParsedEndpoint local_endpoint;
  ibv_device** device_list = nullptr;
  int device_count = 0;
  ibv_context* context = nullptr;
  ibv_pd* pd = nullptr;
  ibv_port_attr port_attr = {};
  ibv_gid gid = {};
  bool use_global = false;
  std::atomic<bool> running{false};
  int listen_fd = -1;
  std::thread accept_thread;
  std::mutex mr_mu;
  std::unordered_map<BufferId, LocalMemoryRegion> local_mrs;
  std::mutex conn_mu;
  std::unordered_map<SegmentId, std::unique_ptr<Connection>> active_connections;
  std::vector<std::unique_ptr<Connection>> passive_connections;
};

RdmaTransportBackend::RdmaTransportBackend() : impl_(std::make_unique<Impl>()) {}

RdmaTransportBackend::~RdmaTransportBackend() { stop(); }

std::string RdmaTransportBackend::name() const { return "rdma"; }

TransportCapabilities RdmaTransportBackend::capabilities() const {
  return TransportCapabilities{
      .supports_read = true,
      .supports_write = true,
      .supports_cuda_buffer =
#ifdef NANO_HAS_CUDA
          true,
#else
          false,
#endif
  };
}

namespace {
QueuePairWire MakeLocalQueuePairWire(
    ibv_qp* qp, const ibv_port_attr& port_attr, const ibv_gid& gid,
    int gid_index, bool use_global) {
  QueuePairWire wire{};
  wire.qp_num = qp->qp_num;
  wire.psn = RandomPsn();
  wire.lid = port_attr.lid;
  wire.gid_index = static_cast<std::uint8_t>(gid_index);
  wire.use_global = use_global ? 1 : 0;
  std::memcpy(wire.gid, gid.raw, sizeof(wire.gid));
  return wire;
}

void ModifyQueuePairToInit(ibv_qp* qp, int port_num) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = static_cast<std::uint8_t>(port_num);
  attr.qp_access_flags =
      IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  EnsureSuccess(
      ibv_modify_qp(qp, &attr,
                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS),
      "failed to move RDMA queue pair to INIT");
}

void ModifyQueuePairToRtr(
    ibv_qp* qp, const QueuePairWire& remote, int port_num) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_1024;
  attr.dest_qp_num = remote.qp_num;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;
  attr.ah_attr.is_global = remote.use_global;
  attr.ah_attr.dlid = remote.lid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = static_cast<std::uint8_t>(port_num);
  if (remote.use_global != 0) {
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = remote.gid_index;
    std::memcpy(
        &attr.ah_attr.grh.dgid, remote.gid, sizeof(attr.ah_attr.grh.dgid));
  }
  EnsureSuccess(
      ibv_modify_qp(qp, &attr,
                    IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER),
      "failed to move RDMA queue pair to RTR");
}

void ModifyQueuePairToRts(ibv_qp* qp, const QueuePairWire& local) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = local.psn;
  attr.max_rd_atomic = 1;
  EnsureSuccess(
      ibv_modify_qp(qp, &attr,
                    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                        IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                        IBV_QP_MAX_QP_RD_ATOMIC),
      "failed to move RDMA queue pair to RTS");
}

std::unique_ptr<RdmaTransportBackend::Impl::Connection> CreateConnection(
    RdmaTransportBackend::Impl& impl) {
  auto connection = std::make_unique<RdmaTransportBackend::Impl::Connection>();
  connection->cq = ibv_create_cq(impl.context, 16, nullptr, nullptr, 0);
  if (connection->cq == nullptr) {
    throw std::runtime_error("failed to create RDMA completion queue");
  }

  ibv_qp_init_attr qp_attr{};
  qp_attr.send_cq = connection->cq;
  qp_attr.recv_cq = connection->cq;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = 16;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;
  connection->qp = ibv_create_qp(impl.pd, &qp_attr);
  if (connection->qp == nullptr) {
    throw std::runtime_error("failed to create RDMA queue pair");
  }

  ModifyQueuePairToInit(connection->qp, impl.local_endpoint.port_num);
  connection->local = MakeLocalQueuePairWire(
      connection->qp, impl.port_attr, impl.gid, impl.local_endpoint.gid_index,
      impl.use_global);
  return connection;
}

void WaitForCompletion(ibv_cq* cq) {
  while (true) {
    ibv_wc wc{};
    int rc = ibv_poll_cq(cq, 1, &wc);
    if (rc < 0) {
      throw std::runtime_error("failed to poll RDMA completion queue");
    }
    if (rc == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      throw std::runtime_error(
          "RDMA work completion failed: " + CompletionError(wc.status));
    }
    return;
  }
}

const RdmaTransportBackend::Impl::LocalMemoryRegion& LookupLocalMemoryRegion(
    RdmaTransportBackend::Impl& impl, BufferId buffer_id) {
  std::lock_guard<std::mutex> lock(impl.mr_mu);
  auto it = impl.local_mrs.find(buffer_id);
  if (it == impl.local_mrs.end()) {
    throw std::runtime_error("RDMA local buffer is not registered");
  }
  return it->second;
}

RdmaTransportBackend::Impl::Connection* LookupActiveConnection(
    RdmaTransportBackend::Impl& impl, SegmentId segment_id) {
  std::lock_guard<std::mutex> lock(impl.conn_mu);
  auto it = impl.active_connections.find(segment_id);
  if (it == impl.active_connections.end()) {
    throw std::runtime_error("RDMA segment connection is not prepared");
  }
  return it->second.get();
}

#ifdef NANO_HAS_CUDA
bool IsCudaDevicePointer(void* addr) {
  CUmemorytype memory_type = CU_MEMORYTYPE_HOST;
  auto result = cuPointerGetAttribute(
      &memory_type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
      reinterpret_cast<CUdeviceptr>(addr));
  return result == CUDA_SUCCESS && memory_type == CU_MEMORYTYPE_DEVICE;
}

ibv_mr* RegisterCudaMemoryRegion(
    ibv_pd* pd, void* addr, std::size_t bytes, int access) {
  if (GetBoolEnv("WITH_NVIDIA_PEERMEM", true)) {
    auto* mr = ibv_reg_mr(pd, addr, bytes, access);
    if (mr == nullptr) {
      throw std::runtime_error(
          "failed to register CUDA memory with ibv_reg_mr; check nvidia-peermem");
    }
    return mr;
  }

  unsigned int device_ordinal = 0;
  EnsureSuccess(
      cuPointerGetAttribute(&device_ordinal,
                            CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                            reinterpret_cast<CUdeviceptr>(addr)),
      "failed to query CUDA device ordinal for RDMA registration");
  CUdevice cu_device = 0;
  CUcontext primary_ctx = nullptr;
  EnsureSuccess(cuDeviceGet(&cu_device, static_cast<int>(device_ordinal)),
                "failed to get CUDA device for RDMA registration");
  EnsureSuccess(cuDevicePrimaryCtxRetain(&primary_ctx, cu_device),
                "failed to retain CUDA primary context for RDMA registration");
  CUcontext previous_ctx = nullptr;
  cuCtxGetCurrent(&previous_ctx);

  auto release_ctx = [&]() {
    if (previous_ctx != nullptr) {
      cuCtxSetCurrent(previous_ctx);
    }
    if (primary_ctx != nullptr) {
      cuDevicePrimaryCtxRelease(cu_device);
      primary_ctx = nullptr;
    }
  };

  try {
    EnsureSuccess(cuCtxSetCurrent(primary_ctx),
                  "failed to set CUDA context for RDMA registration");
    CUdeviceptr alloc_base = 0;
    std::size_t alloc_size = 0;
    EnsureSuccess(
        cuMemGetAddressRange(
            &alloc_base, &alloc_size, reinterpret_cast<CUdeviceptr>(addr)),
        "failed to query CUDA allocation range for RDMA registration");

    int dmabuf_fd = -1;
#ifdef CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD
    EnsureSuccess(
        cuMemGetHandleForAddressRange(
            &dmabuf_fd, alloc_base, alloc_size,
            CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0),
        "failed to export CUDA allocation as DMA-BUF for RDMA registration");
#else
    throw std::runtime_error(
        "CUDA driver headers do not expose DMA-BUF export; rebuild with newer CUDA or use WITH_NVIDIA_PEERMEM=1");
#endif

    const auto dmabuf_offset = static_cast<std::uint64_t>(
        reinterpret_cast<std::uintptr_t>(addr) -
        static_cast<std::uintptr_t>(alloc_base));
    auto* mr = ibv_reg_dmabuf_mr(
        pd, dmabuf_offset, bytes, reinterpret_cast<std::uint64_t>(addr),
        dmabuf_fd, access);
    const int saved_errno = errno;
    ::close(dmabuf_fd);
    errno = saved_errno;
    if (mr == nullptr) {
      throw std::runtime_error(
          "failed to register CUDA DMA-BUF MR; check GPU Direct RDMA / DMA-BUF support");
    }
    release_ctx();
    return mr;
  } catch (...) {
    release_ctx();
    throw;
  }
}
#endif

RdmaTransportBackend::Impl::LocalMemoryRegion RegisterLocalMemoryRegion(
    RdmaTransportBackend::Impl& impl, const RegisteredBuffer& buffer) {
  RdmaTransportBackend::Impl::LocalMemoryRegion registered;
  registered.addr = buffer.view.data;
  registered.bytes = buffer.view.bytes;
  registered.device = buffer.view.device;

  if (buffer.view.device == DeviceType::kCPU) {
    registered.mr =
        ibv_reg_mr(impl.pd, buffer.view.data, buffer.view.bytes, kBaseAccessFlags);
    if (registered.mr == nullptr) {
      throw std::runtime_error("failed to register RDMA local CPU memory region");
    }
    return registered;
  }

  if (buffer.view.device != DeviceType::kCUDA) {
    throw std::invalid_argument("unsupported device type for RDMA registration");
  }

#ifndef NANO_HAS_CUDA
  throw std::runtime_error(
      "CUDA buffer requested for RDMA, but nano-MoonCake was built without CUDA driver support");
#else
  if (!IsCudaDevicePointer(buffer.view.data)) {
    throw std::invalid_argument(
        "buffer is marked as CUDA but does not appear to be a CUDA device pointer");
  }
  registered.mr =
      RegisterCudaMemoryRegion(impl.pd, buffer.view.data, buffer.view.bytes,
                               kBaseAccessFlags);
  return registered;
#endif
}

}  // namespace

void RdmaTransportBackend::do_start(const std::string& local_endpoint) {
  if (impl_->running) {
    throw std::runtime_error("RdmaTransportBackend already started");
  }

  auto cleanup_partial_start = [this]() {
    if (impl_->listen_fd >= 0) {
      ::close(impl_->listen_fd);
      impl_->listen_fd = -1;
    }
    if (impl_->pd != nullptr) {
      ibv_dealloc_pd(impl_->pd);
      impl_->pd = nullptr;
    }
    if (impl_->context != nullptr) {
      ibv_close_device(impl_->context);
      impl_->context = nullptr;
    }
    if (impl_->device_list != nullptr) {
      ibv_free_device_list(impl_->device_list);
      impl_->device_list = nullptr;
      impl_->device_count = 0;
    }
  };

  try {
    impl_->local_endpoint = ParseEndpoint(local_endpoint);
#ifdef NANO_HAS_CUDA
    EnsureSuccess(cuInit(0), "failed to initialize CUDA driver for RDMA transport");
#endif
    if (impl_->local_endpoint.device_name.empty()) {
      if (const char* env = std::getenv("NANO_MOONCAKE_RDMA_DEVICE")) {
        impl_->local_endpoint.device_name = env;
      }
    }
    if (const char* env = std::getenv("NANO_MOONCAKE_RDMA_GID_INDEX")) {
      impl_->local_endpoint.gid_index = std::stoi(env);
    }

    impl_->device_list = ibv_get_device_list(&impl_->device_count);
    if (impl_->device_list == nullptr || impl_->device_count == 0) {
      throw std::runtime_error("no RDMA devices found");
    }

    ibv_device* selected = nullptr;
    for (int i = 0; i < impl_->device_count; ++i) {
      auto* candidate = impl_->device_list[i];
      if (impl_->local_endpoint.device_name.empty() ||
          impl_->local_endpoint.device_name ==
              ibv_get_device_name(candidate)) {
        selected = candidate;
        break;
      }
    }
    if (selected == nullptr) {
      throw std::runtime_error("requested RDMA device was not found");
    }

    impl_->context = ibv_open_device(selected);
    if (impl_->context == nullptr) {
      throw std::runtime_error("failed to open RDMA device");
    }
    impl_->pd = ibv_alloc_pd(impl_->context);
    if (impl_->pd == nullptr) {
      throw std::runtime_error("failed to allocate RDMA protection domain");
    }
    EnsureSuccess(
        ibv_query_port(impl_->context,
                       static_cast<std::uint8_t>(impl_->local_endpoint.port_num),
                       &impl_->port_attr),
        "failed to query RDMA port attributes");
    if (ibv_query_gid(
            impl_->context,
            static_cast<std::uint8_t>(impl_->local_endpoint.port_num),
            impl_->local_endpoint.gid_index, &impl_->gid) == 0) {
      impl_->use_global = !GidIsZero(impl_->gid);
    }

    impl_->listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (impl_->listen_fd < 0) {
      throw std::runtime_error("failed to create RDMA handshake listener");
    }
    const int reuse = 1;
    ::setsockopt(
        impl_->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(impl_->local_endpoint.port);
    if (::inet_pton(
            AF_INET, impl_->local_endpoint.host.c_str(), &addr.sin_addr) != 1) {
      throw std::invalid_argument("RDMA local host must be an IPv4 address");
    }
    if (::bind(impl_->listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) !=
        0) {
      throw std::runtime_error("failed to bind RDMA handshake listener");
    }
    if (::listen(impl_->listen_fd, 16) != 0) {
      throw std::runtime_error("failed to listen on RDMA handshake socket");
    }

    impl_->running = true;
    impl_->accept_thread = std::thread([this]() {
      while (impl_->running) {
        int client_fd = ::accept(impl_->listen_fd, nullptr, nullptr);
        if (client_fd < 0) {
          if (!impl_->running) {
            break;
          }
          continue;
        }

        try {
          PrepareHeaderWire header{};
          ReadFully(client_fd, &header, sizeof(header));
          if (header.magic != kHandshakeMagic ||
              header.version != kHandshakeVersion) {
            throw std::runtime_error("invalid RDMA handshake header");
          }

          TraceContextScope trace_scope(header.trace_id);
          std::string segment_name(header.name_length, '\0');
          if (header.name_length > 0) {
            ReadFully(client_fd, segment_name.data(), header.name_length);
          }

          auto segment = get_exported_segment(segment_name);
          auto buffer = get_registered_buffer(segment.buffer_id);
          const auto& mr = LookupLocalMemoryRegion(*impl_, segment.buffer_id);

          auto connection = CreateConnection(*impl_);
          connection->segment_name = segment_name;
          connection->remote = header.initiator;
          ModifyQueuePairToRtr(
              connection->qp, connection->remote, impl_->local_endpoint.port_num);
          ModifyQueuePairToRts(connection->qp, connection->local);

          PrepareReplyWire reply{};
          reply.status = kStatusOk;
          reply.remote_key = mr.mr->rkey;
          reply.remote_base_addr =
              reinterpret_cast<std::uint64_t>(buffer.view.data) +
              segment.base_offset;
          reply.remote_bytes = segment.bytes;
          reply.responder = connection->local;
          std::strncpy(reply.message, "ok", sizeof(reply.message) - 1);
          WriteFully(client_fd, &reply, sizeof(reply));

          std::lock_guard<std::mutex> lock(impl_->conn_mu);
          impl_->passive_connections.push_back(std::move(connection));
        } catch (...) {
          try {
            PrepareReplyWire reply{};
            reply.status = kStatusError;
            auto msg = std::current_exception();
            try {
              if (msg) {
                std::rethrow_exception(msg);
              }
            } catch (const std::exception& ex) {
              std::strncpy(reply.message, ex.what(), sizeof(reply.message) - 1);
            }
            WriteFully(client_fd, &reply, sizeof(reply));
          } catch (...) {
          }
        }
        ::close(client_fd);
      }
    });
  } catch (...) {
    cleanup_partial_start();
    throw;
  }
}

void RdmaTransportBackend::do_stop() {
  if (!impl_->running) {
    return;
  }
  impl_->running = false;
  if (impl_->listen_fd >= 0) {
    ::shutdown(impl_->listen_fd, SHUT_RDWR);
    ::close(impl_->listen_fd);
    impl_->listen_fd = -1;
  }
  if (impl_->accept_thread.joinable()) {
    impl_->accept_thread.join();
  }

  {
    std::lock_guard<std::mutex> lock(impl_->conn_mu);
    impl_->active_connections.clear();
    impl_->passive_connections.clear();
  }
  {
    std::lock_guard<std::mutex> lock(impl_->mr_mu);
    for (auto& [_, region] : impl_->local_mrs) {
      ibv_dereg_mr(region.mr);
    }
    impl_->local_mrs.clear();
  }
  if (impl_->pd != nullptr) {
    ibv_dealloc_pd(impl_->pd);
    impl_->pd = nullptr;
  }
  if (impl_->context != nullptr) {
    ibv_close_device(impl_->context);
    impl_->context = nullptr;
  }
  if (impl_->device_list != nullptr) {
    ibv_free_device_list(impl_->device_list);
    impl_->device_list = nullptr;
    impl_->device_count = 0;
  }
}

void RdmaTransportBackend::do_add_local_buffer(const RegisteredBuffer& buffer) {
  auto region = RegisterLocalMemoryRegion(*impl_, buffer);
  std::lock_guard<std::mutex> lock(impl_->mr_mu);
  impl_->local_mrs[buffer.buffer_id] = std::move(region);
}

void RdmaTransportBackend::do_remove_local_buffer(const RegisteredBuffer& buffer) {
  std::lock_guard<std::mutex> lock(impl_->mr_mu);
  auto it = impl_->local_mrs.find(buffer.buffer_id);
  if (it == impl_->local_mrs.end()) {
    return;
  }
  ibv_dereg_mr(it->second.mr);
  impl_->local_mrs.erase(it);
}

void RdmaTransportBackend::do_remove_local_segment(const MountedSegment& segment) {
  std::lock_guard<std::mutex> lock(impl_->conn_mu);
  auto keep = impl_->passive_connections.begin();
  for (auto it = impl_->passive_connections.begin();
       it != impl_->passive_connections.end(); ++it) {
    if ((*it)->segment_name == segment.segment_name) {
      it->reset();
      continue;
    }
    if (keep != it) {
      *keep = std::move(*it);
    }
    ++keep;
  }
  impl_->passive_connections.erase(keep, impl_->passive_connections.end());
}

void RdmaTransportBackend::do_prepare_segment(RemoteSegmentHandle& segment) {
  const auto total_start = TraceNow();
  auto endpoint = ParseEndpoint(segment.peer_endpoint);
  const auto connect_start = TraceNow();
  int fd = ConnectTo(endpoint);
  const auto connect_end = TraceNow();
  LogTrace("rdma_transport", "prepare_segment_connect",
           TraceFields{
               .segment_id = segment.segment_id,
               .duration_us = ElapsedUs(connect_start, connect_end),
               .segment_name = segment.segment_name,
               .endpoint = segment.peer_endpoint,
               .status = "ok",
           });
  try {
    auto connection = CreateConnection(*impl_);
    connection->segment_name = segment.segment_name;

    PrepareHeaderWire header{};
    header.magic = kHandshakeMagic;
    header.version = kHandshakeVersion;
    header.name_length = static_cast<std::uint16_t>(segment.segment_name.size());
    header.trace_id = EnsureTraceId();
    header.initiator = connection->local;

    WriteFully(fd, &header, sizeof(header));
    if (!segment.segment_name.empty()) {
      WriteFully(fd, segment.segment_name.data(), segment.segment_name.size());
    }

    PrepareReplyWire reply{};
    ReadFully(fd, &reply, sizeof(reply));
    if (reply.status != kStatusOk) {
      throw std::runtime_error(
          "RDMA handshake failed: " + TrimMessage(reply.message, sizeof(reply.message)));
    }

    connection->remote = reply.responder;
    connection->remote_key = reply.remote_key;
    ModifyQueuePairToRtr(
        connection->qp, connection->remote, impl_->local_endpoint.port_num);
    ModifyQueuePairToRts(connection->qp, connection->local);

    segment.remote_base_addr = reply.remote_base_addr;
    segment.remote_bytes = static_cast<std::size_t>(reply.remote_bytes);
    segment.remote_key = reply.remote_key;

    {
      std::lock_guard<std::mutex> lock(impl_->conn_mu);
      impl_->active_connections[segment.segment_id] = std::move(connection);
    }
  } catch (...) {
    ::close(fd);
    LogTrace("rdma_transport", "prepare_segment",
             TraceFields{
                 .segment_id = segment.segment_id,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .segment_name = segment.segment_name,
                 .endpoint = segment.peer_endpoint,
                 .status = "error",
             });
    throw;
  }
  ::close(fd);
  LogTrace("rdma_transport", "prepare_segment",
           TraceFields{
               .segment_id = segment.segment_id,
               .bytes = segment.remote_bytes,
               .duration_us = ElapsedUs(total_start, TraceNow()),
               .segment_name = segment.segment_name,
               .endpoint = segment.peer_endpoint,
               .status = "ok",
           });
}

void RdmaTransportBackend::do_submit_request(
    const ResolvedTransferRequest& request) {
  const auto total_start = TraceNow();
  auto* connection = LookupActiveConnection(*impl_, request.remote.segment.segment_id);
  const auto& mr = LookupLocalMemoryRegion(*impl_, request.local_buffer_id);

  ibv_sge sge{};
  sge.addr = reinterpret_cast<std::uint64_t>(request.local_view.data);
  sge.length = static_cast<std::uint32_t>(request.length);
  sge.lkey = mr.mr->lkey;

  ibv_send_wr wr{};
  wr.wr_id = request.request_id;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = request.opcode == TransferOpcode::kRead ? IBV_WR_RDMA_READ
                                                       : IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = request.resolved_remote_addr;
  wr.wr.rdma.rkey = connection->remote_key;

  ibv_send_wr* bad_wr = nullptr;
  if (ibv_post_send(connection->qp, &wr, &bad_wr) != 0) {
    throw std::runtime_error("failed to post RDMA work request");
  }
  WaitForCompletion(connection->cq);

  LogTrace("rdma_transport", "submit_request",
           TraceFields{
               .request_id = request.request_id,
               .segment_id = request.remote.segment.segment_id,
               .bytes = request.length,
               .duration_us = ElapsedUs(total_start, TraceNow()),
               .opcode = TransferOpcodeName(
                   static_cast<std::uint8_t>(request.opcode)),
               .segment_name = request.remote.segment.segment_name,
               .endpoint = request.peer_endpoint,
               .status = "ok",
           });
}

void RdmaTransportBackend::release_segment(SegmentId segment_id) {
  std::lock_guard<std::mutex> lock(impl_->conn_mu);
  auto it = impl_->active_connections.find(segment_id);
  if (it == impl_->active_connections.end()) {
    return;
  }
  impl_->active_connections.erase(it);
}

}  // namespace nano_mooncake

#endif  // NANO_HAS_RDMA
