#include "nano_mooncake/tcp_transport.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace nano_mooncake {

namespace {
constexpr std::uint32_t kWireMagic = 0x4e4d4f4f;  // "NMOO"
constexpr std::uint16_t kWireVersion = 1;
constexpr std::uint16_t kDescribeOp = 1;
constexpr std::uint16_t kReadOp = 2;
constexpr std::uint16_t kWriteOp = 3;
constexpr std::uint32_t kStatusOk = 0;
constexpr std::uint32_t kStatusError = 1;
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
  auto endpoint = parse_endpoint(segment.peer_endpoint);
  int fd = connect_to(endpoint);
  try {
    WireHeader header{
        .magic = kWireMagic,
        .version = kWireVersion,
        .opcode = kDescribeOp,
        .name_length = static_cast<std::uint16_t>(segment.segment_name.size()),
        .reserved = 0,
        .offset = 0,
        .length = 0,
    };
    write_fully(fd, &header, sizeof(header));
    if (!segment.segment_name.empty()) {
      write_fully(fd, segment.segment_name.data(), segment.segment_name.size());
    }

    DescribeReply reply{};
    read_fully(fd, &reply, sizeof(reply));
    if (reply.status != kStatusOk) {
      throw std::runtime_error(
          "remote describe failed: " + trim_message(reply.message, sizeof(reply.message)));
    }

    segment.remote_base_addr = reply.remote_base_addr;
    segment.remote_bytes = static_cast<std::size_t>(reply.remote_bytes);
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
}

void TcpTransportBackend::do_submit_request(
    const ResolvedTransferRequest& request) {
  auto endpoint = parse_endpoint(request.peer_endpoint);
  int fd = connect_to(endpoint);
  try {
    WireHeader header{
        .magic = kWireMagic,
        .version = kWireVersion,
        .opcode = request.opcode == TransferOpcode::kRead ? kReadOp : kWriteOp,
        .name_length = static_cast<std::uint16_t>(request.remote.segment.segment_name.size()),
        .reserved = 0,
        .offset = request.remote.offset,
        .length = request.length,
    };
    write_fully(fd, &header, sizeof(header));
    if (!request.remote.segment.segment_name.empty()) {
      write_fully(
          fd, request.remote.segment.segment_name.data(),
          request.remote.segment.segment_name.size());
    }

    DescribeReply reply{};
    if (request.opcode == TransferOpcode::kWrite) {
      write_fully(fd, request.local_view.data, request.length);
      read_fully(fd, &reply, sizeof(reply));
    } else {
      read_fully(fd, &reply, sizeof(reply));
      if (reply.status == kStatusOk) {
        read_fully(fd, request.local_view.data, request.length);
      }
    }

    if (reply.status != kStatusOk) {
      throw std::runtime_error(
          "remote transfer failed: " + trim_message(reply.message, sizeof(reply.message)));
    }
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
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
  WireHeader header{};
  read_fully(client_fd, &header, sizeof(header));
  if (header.magic != kWireMagic || header.version != kWireVersion) {
    throw std::runtime_error("invalid wire header");
  }

  std::string segment_name(header.name_length, '\0');
  if (header.name_length > 0) {
    read_fully(client_fd, segment_name.data(), header.name_length);
  }

  auto segment = get_exported_segment(segment_name);
  auto buffer = get_registered_buffer(segment.buffer_id);
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
    write_fully(client_fd, &reply, sizeof(reply));
  };

  if (header.opcode == kDescribeOp) {
    write_reply(kStatusOk, segment.bytes, "ok");
    return;
  }
  if (offset > segment.bytes || length > segment.bytes - offset) {
    write_reply(kStatusError, 0, "remote range exceeds mounted segment");
    return;
  }

  auto* target = base + segment.base_offset + offset;
  if (header.opcode == kWriteOp) {
    read_fully(client_fd, target, length);
    write_reply(kStatusOk, length, "ok");
    return;
  }
  if (header.opcode == kReadOp) {
    write_reply(kStatusOk, length, "ok");
    write_fully(client_fd, target, length);
    return;
  }
  write_reply(kStatusError, 0, "unsupported opcode");
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
