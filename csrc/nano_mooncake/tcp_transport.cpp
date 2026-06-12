#include "nano_mooncake/tcp_transport.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

#include "nano_mooncake/observability.h"

namespace nano_mooncake {

namespace {
constexpr std::uint32_t kWireMagic = 0x4e4d4f4f;  // "NMOO"
constexpr std::uint16_t kWireVersion = 2;
constexpr std::uint16_t kDescribeOp = 1;
constexpr std::uint16_t kReadOp = 2;
constexpr std::uint16_t kWriteOp = 3;
constexpr std::uint32_t kStatusOk = 0;
constexpr std::uint32_t kStatusError = 1;

const char* TcpOpcodeName(std::uint16_t opcode) {
  switch (opcode) {
    case kDescribeOp:
      return "describe";
    case kReadOp:
      return "read";
    case kWriteOp:
      return "write";
    default:
      return "unknown";
  }
}
}

TcpTransportBackend::~TcpTransportBackend() { stop(); }

std::string TcpTransportBackend::name() const { return "tcp"; }

TransportCapabilities TcpTransportBackend::capabilities() const {
  return TransportCapabilities{
      .supports_read = true,
      .supports_write = true,
      .supports_cuda_buffer = false,
  };
}

void TcpTransportBackend::do_start(const std::string& local_endpoint) {
  if (running_) {
    throw std::runtime_error("TcpTransportBackend already started");
  }

  local_endpoint_ = parse_endpoint(local_endpoint);
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    throw std::runtime_error("failed to create TCP socket");
  }

  const int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(local_endpoint_.port);
  if (::inet_pton(AF_INET, local_endpoint_.host.c_str(), &addr.sin_addr) != 1) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::invalid_argument("local_endpoint host must be an IPv4 address");
  }
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to bind TCP listener");
  }
  if (::listen(listen_fd_, 16) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to listen on TCP socket");
  }

  running_ = true;
  accept_thread_ = std::thread(&TcpTransportBackend::accept_loop, this);
}

void TcpTransportBackend::do_stop() {
  if (!running_) {
    return;
  }
  running_ = false;
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
}

void TcpTransportBackend::do_prepare_segment(RemoteSegmentHandle& segment) {
  const auto total_start = TraceNow();
  const auto connect_start = TraceNow();
  auto endpoint = parse_endpoint(segment.peer_endpoint);
  int fd = connect_to(endpoint);
  const auto connect_end = TraceNow();
  LogTrace("tcp_transport", "prepare_segment_connect",
           TraceFields{
               .segment_id = segment.segment_id,
               .duration_us = ElapsedUs(connect_start, connect_end),
               .segment_name = segment.segment_name,
               .endpoint = segment.peer_endpoint,
               .status = "ok",
           });
  try {
    WireHeader header{
        .magic = kWireMagic,
        .version = kWireVersion,
        .opcode = kDescribeOp,
        .name_length = static_cast<std::uint16_t>(segment.segment_name.size()),
        .reserved = 0,
        .trace_id = EnsureTraceId(),
        .request_id = 0,
        .offset = 0,
        .length = 0,
    };
    const auto write_start = TraceNow();
    write_fully(fd, &header, sizeof(header));
    if (!segment.segment_name.empty()) {
      write_fully(fd, segment.segment_name.data(), segment.segment_name.size());
    }
    const auto write_end = TraceNow();
    LogTrace("tcp_transport", "prepare_segment_write",
             TraceFields{
                 .bytes = sizeof(header) + segment.segment_name.size(),
                 .segment_id = segment.segment_id,
                 .duration_us = ElapsedUs(write_start, write_end),
                 .segment_name = segment.segment_name,
                 .endpoint = segment.peer_endpoint,
                 .status = "ok",
             });

    DescribeReply reply{};
    const auto read_start = TraceNow();
    read_fully(fd, &reply, sizeof(reply));
    const auto read_end = TraceNow();
    LogTrace("tcp_transport", "prepare_segment_read",
             TraceFields{
                 .bytes = sizeof(reply),
                 .segment_id = segment.segment_id,
                 .duration_us = ElapsedUs(read_start, read_end),
                 .segment_name = segment.segment_name,
                 .endpoint = segment.peer_endpoint,
                 .status = reply.status == kStatusOk ? "ok" : "error",
                 .message = trim_message(reply.message, sizeof(reply.message)),
             });
    if (reply.status != kStatusOk) {
      throw std::runtime_error(
          "remote describe failed: " + trim_message(reply.message, sizeof(reply.message)));
    }

    segment.remote_base_addr = reply.remote_base_addr;
    segment.remote_bytes = static_cast<std::size_t>(reply.remote_bytes);
  } catch (...) {
    LogTrace("tcp_transport", "prepare_segment",
             TraceFields{
                 .segment_id = segment.segment_id,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .segment_name = segment.segment_name,
                 .endpoint = segment.peer_endpoint,
                 .status = "error",
             });
    ::close(fd);
    throw;
  }
  ::close(fd);
  LogTrace("tcp_transport", "prepare_segment",
           TraceFields{
               .segment_id = segment.segment_id,
               .bytes = segment.remote_bytes,
               .duration_us = ElapsedUs(total_start, TraceNow()),
               .segment_name = segment.segment_name,
               .endpoint = segment.peer_endpoint,
               .status = "ok",
           });
}

void TcpTransportBackend::do_submit_request(
    const ResolvedTransferRequest& request) {
  const auto total_start = TraceNow();
  auto endpoint = parse_endpoint(request.peer_endpoint);
  const auto connect_start = TraceNow();
  int fd = connect_to(endpoint);
  const auto connect_end = TraceNow();
  LogTrace("tcp_transport", "request_connect",
           TraceFields{
               .request_id = request.request_id,
               .segment_id = request.remote.segment.segment_id,
               .bytes = request.length,
               .duration_us = ElapsedUs(connect_start, connect_end),
               .opcode = TransferOpcodeName(
                   static_cast<std::uint8_t>(request.opcode)),
               .segment_name = request.remote.segment.segment_name,
               .endpoint = request.peer_endpoint,
               .status = "ok",
           });
  try {
    WireHeader header{
        .magic = kWireMagic,
        .version = kWireVersion,
        .opcode = request.opcode == TransferOpcode::kRead ? kReadOp : kWriteOp,
        .name_length = static_cast<std::uint16_t>(request.remote.segment.segment_name.size()),
        .reserved = 0,
        .trace_id = EnsureTraceId(),
        .request_id = request.request_id,
        .offset = request.remote.offset,
        .length = request.length,
    };
    const auto write_meta_start = TraceNow();
    write_fully(fd, &header, sizeof(header));
    if (!request.remote.segment.segment_name.empty()) {
      write_fully(
          fd, request.remote.segment.segment_name.data(),
          request.remote.segment.segment_name.size());
    }
    const auto write_meta_end = TraceNow();
    LogTrace("tcp_transport", "request_write_meta",
             TraceFields{
                 .request_id = request.request_id,
                 .segment_id = request.remote.segment.segment_id,
                 .bytes = sizeof(header) + request.remote.segment.segment_name.size(),
                 .duration_us = ElapsedUs(write_meta_start, write_meta_end),
                 .opcode = TransferOpcodeName(
                     static_cast<std::uint8_t>(request.opcode)),
                 .segment_name = request.remote.segment.segment_name,
                 .endpoint = request.peer_endpoint,
                 .status = "ok",
             });

    DescribeReply reply{};
    if (request.opcode == TransferOpcode::kWrite) {
      const auto payload_write_start = TraceNow();
      write_fully(fd, request.local_view.data, request.length);
      const auto payload_write_end = TraceNow();
      LogTrace("tcp_transport", "request_write_payload",
               TraceFields{
                   .request_id = request.request_id,
                   .segment_id = request.remote.segment.segment_id,
                   .bytes = request.length,
                   .duration_us = ElapsedUs(payload_write_start, payload_write_end),
                   .opcode = "write",
                   .segment_name = request.remote.segment.segment_name,
                   .endpoint = request.peer_endpoint,
                   .status = "ok",
               });
      const auto ack_read_start = TraceNow();
      read_fully(fd, &reply, sizeof(reply));
      const auto ack_read_end = TraceNow();
      LogTrace("tcp_transport", "request_read_ack",
               TraceFields{
                   .request_id = request.request_id,
                   .segment_id = request.remote.segment.segment_id,
                   .bytes = sizeof(reply),
                   .duration_us = ElapsedUs(ack_read_start, ack_read_end),
                   .opcode = "write",
                   .segment_name = request.remote.segment.segment_name,
                   .endpoint = request.peer_endpoint,
                   .status = reply.status == kStatusOk ? "ok" : "error",
                   .message = trim_message(reply.message, sizeof(reply.message)),
               });
    } else {
      const auto ack_read_start = TraceNow();
      read_fully(fd, &reply, sizeof(reply));
      const auto ack_read_end = TraceNow();
      LogTrace("tcp_transport", "request_read_ack",
               TraceFields{
                   .request_id = request.request_id,
                   .segment_id = request.remote.segment.segment_id,
                   .bytes = sizeof(reply),
                   .duration_us = ElapsedUs(ack_read_start, ack_read_end),
                   .opcode = "read",
                   .segment_name = request.remote.segment.segment_name,
                   .endpoint = request.peer_endpoint,
                   .status = reply.status == kStatusOk ? "ok" : "error",
                   .message = trim_message(reply.message, sizeof(reply.message)),
               });
      if (reply.status == kStatusOk) {
        const auto payload_read_start = TraceNow();
        read_fully(fd, request.local_view.data, request.length);
        const auto payload_read_end = TraceNow();
        LogTrace("tcp_transport", "request_read_payload",
                 TraceFields{
                     .request_id = request.request_id,
                     .segment_id = request.remote.segment.segment_id,
                     .bytes = request.length,
                     .duration_us = ElapsedUs(payload_read_start, payload_read_end),
                     .opcode = "read",
                     .segment_name = request.remote.segment.segment_name,
                     .endpoint = request.peer_endpoint,
                     .status = "ok",
                 });
      }
    }

    if (reply.status != kStatusOk) {
      throw std::runtime_error(
          "remote transfer failed: " + trim_message(reply.message, sizeof(reply.message)));
    }
  } catch (...) {
    LogTrace("tcp_transport", "submit_request",
             TraceFields{
                 .request_id = request.request_id,
                 .segment_id = request.remote.segment.segment_id,
                 .bytes = request.length,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .opcode = TransferOpcodeName(
                     static_cast<std::uint8_t>(request.opcode)),
                 .segment_name = request.remote.segment.segment_name,
                 .endpoint = request.peer_endpoint,
                 .status = "error",
             });
    ::close(fd);
    throw;
  }
  ::close(fd);
  LogTrace("tcp_transport", "submit_request",
           TraceFields{
               .request_id = request.request_id,
               .segment_id = request.remote.segment.segment_id,
               .bytes = request.length,
               .duration_us = ElapsedUs(total_start, TraceNow()),
               .opcode =
                   TransferOpcodeName(static_cast<std::uint8_t>(request.opcode)),
               .segment_name = request.remote.segment.segment_name,
               .endpoint = request.peer_endpoint,
               .status = "ok",
           });
}

void TcpTransportBackend::accept_loop() {
  while (running_) {
    int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      if (!running_) {
        break;
      }
      continue;
    }
    try {
      handle_client(client_fd);
    } catch (...) {
    }
    ::close(client_fd);
  }
}

void TcpTransportBackend::handle_client(int client_fd) {
  const auto total_start = TraceNow();
  WireHeader header{};
  const auto read_meta_start = TraceNow();
  read_fully(client_fd, &header, sizeof(header));
  const auto read_meta_end = TraceNow();
  if (header.magic != kWireMagic || header.version != kWireVersion) {
    throw std::runtime_error("invalid wire header");
  }
  TraceContextScope trace_scope(header.trace_id);
  LogTrace("tcp_transport_server", "read_meta",
           TraceFields{
               .request_id = header.request_id,
               .bytes = sizeof(header),
               .duration_us = ElapsedUs(read_meta_start, read_meta_end),
               .opcode = TcpOpcodeName(header.opcode),
               .status = "ok",
           });

  std::string segment_name(header.name_length, '\0');
  if (header.name_length > 0) {
    const auto read_name_start = TraceNow();
    read_fully(client_fd, segment_name.data(), header.name_length);
    const auto read_name_end = TraceNow();
    LogTrace("tcp_transport_server", "read_segment_name",
             TraceFields{
                 .request_id = header.request_id,
                 .bytes = header.name_length,
                 .duration_us = ElapsedUs(read_name_start, read_name_end),
                 .opcode = TcpOpcodeName(header.opcode),
                 .segment_name = segment_name,
                 .status = "ok",
             });
  }

  const auto lookup_start = TraceNow();
  auto segment = get_exported_segment(segment_name);
  auto buffer = get_registered_buffer(segment.buffer_id);
  const auto lookup_end = TraceNow();
  LogTrace("tcp_transport_server", "lookup_segment",
           TraceFields{
               .request_id = header.request_id,
               .duration_us = ElapsedUs(lookup_start, lookup_end),
               .opcode = TcpOpcodeName(header.opcode),
               .segment_name = segment_name,
               .status = "ok",
           });
  auto* base = static_cast<std::uint8_t*>(buffer.view.data);
  const std::size_t offset = static_cast<std::size_t>(header.offset);
  const std::size_t length = static_cast<std::size_t>(header.length);

  auto write_reply = [&](std::uint32_t status, std::uint64_t bytes,
                         const std::string& message) {
    DescribeReply reply{};
    reply.status = status;
    reply.remote_base_addr = segment.base_offset;
    reply.remote_bytes = bytes;
    std::strncpy(reply.message, message.c_str(), sizeof(reply.message) - 1);
    const auto reply_start = TraceNow();
    write_fully(client_fd, &reply, sizeof(reply));
    const auto reply_end = TraceNow();
    LogTrace("tcp_transport_server", "write_reply",
             TraceFields{
                 .request_id = header.request_id,
                 .bytes = sizeof(reply),
                 .duration_us = ElapsedUs(reply_start, reply_end),
                 .opcode = TcpOpcodeName(header.opcode),
                 .segment_name = segment_name,
                 .status = status == kStatusOk ? "ok" : "error",
                 .message = message,
             });
  };

  if (header.opcode == kDescribeOp) {
    write_reply(kStatusOk, segment.bytes, "ok");
    LogTrace("tcp_transport_server", "handle_request",
             TraceFields{
                 .request_id = header.request_id,
                 .bytes = segment.bytes,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .opcode = "describe",
                 .segment_name = segment_name,
                 .status = "ok",
             });
    return;
  }
  if (offset > segment.bytes || length > segment.bytes - offset) {
    write_reply(kStatusError, 0, "remote range exceeds mounted segment");
    LogTrace("tcp_transport_server", "handle_request",
             TraceFields{
                 .request_id = header.request_id,
                 .offset = offset,
                 .bytes = length,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .opcode = TcpOpcodeName(header.opcode),
                 .segment_name = segment_name,
                 .status = "error",
                 .message = "remote range exceeds mounted segment",
             });
    return;
  }

  auto* target = base + segment.base_offset + offset;
  if (header.opcode == kWriteOp) {
    const auto payload_read_start = TraceNow();
    read_fully(client_fd, target, length);
    const auto payload_read_end = TraceNow();
    LogTrace("tcp_transport_server", "read_payload",
             TraceFields{
                 .request_id = header.request_id,
                 .offset = offset,
                 .bytes = length,
                 .duration_us = ElapsedUs(payload_read_start, payload_read_end),
                 .opcode = "write",
                 .segment_name = segment_name,
                 .status = "ok",
             });
    write_reply(kStatusOk, length, "ok");
    LogTrace("tcp_transport_server", "handle_request",
             TraceFields{
                 .request_id = header.request_id,
                 .offset = offset,
                 .bytes = length,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .opcode = "write",
                 .segment_name = segment_name,
                 .status = "ok",
             });
    return;
  }
  if (header.opcode == kReadOp) {
    write_reply(kStatusOk, length, "ok");
    const auto payload_write_start = TraceNow();
    write_fully(client_fd, target, length);
    const auto payload_write_end = TraceNow();
    LogTrace("tcp_transport_server", "write_payload",
             TraceFields{
                 .request_id = header.request_id,
                 .offset = offset,
                 .bytes = length,
                 .duration_us = ElapsedUs(payload_write_start, payload_write_end),
                 .opcode = "read",
                 .segment_name = segment_name,
                 .status = "ok",
             });
    LogTrace("tcp_transport_server", "handle_request",
             TraceFields{
                 .request_id = header.request_id,
                 .offset = offset,
                 .bytes = length,
                 .duration_us = ElapsedUs(total_start, TraceNow()),
                 .opcode = "read",
                 .segment_name = segment_name,
                 .status = "ok",
             });
    return;
  }
  write_reply(kStatusError, 0, "unsupported opcode");
  LogTrace("tcp_transport_server", "handle_request",
           TraceFields{
               .request_id = header.request_id,
               .duration_us = ElapsedUs(total_start, TraceNow()),
               .opcode = TcpOpcodeName(header.opcode),
               .segment_name = segment_name,
               .status = "error",
               .message = "unsupported opcode",
           });
}

TcpTransportBackend::Endpoint TcpTransportBackend::parse_endpoint(
    const std::string& endpoint) const {
  std::string normalized = endpoint;
  constexpr const char kPrefix[] = "tcp://";
  if (normalized.rfind(kPrefix, 0) == 0) {
    normalized.erase(0, sizeof(kPrefix) - 1);
  }
  auto pos = normalized.rfind(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= normalized.size()) {
    throw std::invalid_argument("endpoint must be in tcp://host:port format");
  }

  Endpoint parsed;
  parsed.host = normalized.substr(0, pos);
  int port = std::stoi(normalized.substr(pos + 1));
  if (port <= 0 || port > 65535) {
    throw std::invalid_argument("endpoint port is out of range");
  }
  parsed.port = static_cast<std::uint16_t>(port);
  return parsed;
}

int TcpTransportBackend::connect_to(const Endpoint& endpoint) const {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create TCP client socket");
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(endpoint.port);
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::invalid_argument("peer host must be an IPv4 address");
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    throw std::runtime_error("failed to connect to remote TCP peer");
  }
  return fd;
}

void TcpTransportBackend::read_fully(int fd, void* data, std::size_t bytes) {
  auto* cursor = static_cast<std::uint8_t*>(data);
  while (bytes > 0) {
    ssize_t received = ::recv(fd, cursor, bytes, 0);
    if (received <= 0) {
      throw std::runtime_error("socket read failed");
    }
    cursor += received;
    bytes -= static_cast<std::size_t>(received);
  }
}

void TcpTransportBackend::write_fully(int fd, const void* data, std::size_t bytes) {
  auto* cursor = static_cast<const std::uint8_t*>(data);
  while (bytes > 0) {
    ssize_t written = ::send(fd, cursor, bytes, 0);
    if (written <= 0) {
      throw std::runtime_error("socket write failed");
    }
    cursor += written;
    bytes -= static_cast<std::size_t>(written);
  }
}

std::string TcpTransportBackend::trim_message(const char* raw, std::size_t bytes) {
  std::size_t length = 0;
  while (length < bytes && raw[length] != '\0') {
    ++length;
  }
  return std::string(raw, length);
}

}  // namespace nano_mooncake
