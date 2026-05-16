#include "nano_mooncake/master_server.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace nano_mooncake {

MasterServer::~MasterServer() { Stop(); }

void MasterServer::Start(const std::string& listen_endpoint) {
  if (running_) {
    throw std::runtime_error("MasterServer already started");
  }
  auto endpoint = ParseEndpoint(listen_endpoint);
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    throw std::runtime_error("failed to create master server socket");
  }
  const int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(endpoint.port);
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &addr.sin_addr) != 1) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::invalid_argument("master listen host must be an IPv4 address");
  }
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to bind master server socket");
  }
  if (::listen(listen_fd_, 32) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to listen on master server socket");
  }
  running_ = true;
  accept_thread_ = std::thread(&MasterServer::AcceptLoop, this);
}

void MasterServer::Stop() {
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

void MasterServer::AcceptLoop() {
  while (running_) {
    int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      if (!running_) {
        break;
      }
      continue;
    }
    try {
      HandleClient(client_fd);
    } catch (...) {
    }
    ::close(client_fd);
  }
}

void MasterServer::HandleClient(int client_fd) {
  auto payload = ReadFrame(client_fd);
  auto request = ParseMasterRequest(payload);
  auto response = Dispatch(request);
  WriteFrame(client_fd, SerializeMasterResponse(response));
}

MasterResponse MasterServer::Dispatch(const MasterRequest& request) {
  try {
    switch (request.opcode) {
      case MasterOpcode::kMountSegment:
        service_.MountSegment(request.segment);
        return MasterResponse{.ok = true, .message = "mounted"};
      case MasterOpcode::kUnmountSegment:
        service_.UnmountSegment(request.segment_name);
        return MasterResponse{.ok = true, .message = "unmounted"};
      case MasterOpcode::kResolveSegment: {
        auto record = service_.ResolveSegment(request.segment_name);
        if (!record.has_value()) {
          return MasterResponse{.ok = false, .message = "segment not found"};
        }
        return MasterResponse{
            .ok = true, .message = "resolved", .segment = record};
      }
      case MasterOpcode::kPutObject:
        service_.PutObject(request.object);
        return MasterResponse{.ok = true, .message = "object stored"};
      case MasterOpcode::kGetObject: {
        auto object = service_.GetObject(request.object_key);
        if (!object.has_value()) {
          return MasterResponse{.ok = false, .message = "object not found"};
        }
        return MasterResponse{
            .ok = true, .message = "object resolved", .object = object};
      }
      case MasterOpcode::kListSegments:
        return MasterResponse{
            .ok = true,
            .message = "listed segments",
            .segments = service_.ListSegments(),
        };
      case MasterOpcode::kListObjects:
        return MasterResponse{
            .ok = true,
            .message = "listed objects",
            .objects = service_.ListObjects(),
        };
    }
  } catch (const std::exception& ex) {
    return MasterResponse{.ok = false, .message = ex.what()};
  }
  return MasterResponse{.ok = false, .message = "unsupported master opcode"};
}

MasterServer::Endpoint MasterServer::ParseEndpoint(const std::string& endpoint) {
  std::string normalized = endpoint;
  constexpr const char kPrefix[] = "tcp://";
  if (normalized.rfind(kPrefix, 0) == 0) {
    normalized.erase(0, sizeof(kPrefix) - 1);
  }
  auto pos = normalized.rfind(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= normalized.size()) {
    throw std::invalid_argument("master endpoint must be in tcp://host:port format");
  }
  Endpoint parsed;
  parsed.host = normalized.substr(0, pos);
  int port = std::stoi(normalized.substr(pos + 1));
  if (port <= 0 || port > 65535) {
    throw std::invalid_argument("master endpoint port is out of range");
  }
  parsed.port = static_cast<std::uint16_t>(port);
  return parsed;
}

std::string MasterServer::ReadFrame(int fd) {
  std::uint32_t length = 0;
  auto* header = reinterpret_cast<std::uint8_t*>(&length);
  std::size_t header_left = sizeof(length);
  while (header_left > 0) {
    ssize_t received = ::recv(fd, header, header_left, 0);
    if (received <= 0) {
      throw std::runtime_error("master server header read failed");
    }
    header += received;
    header_left -= static_cast<std::size_t>(received);
  }

  std::string payload(length, '\0');
  auto* body = reinterpret_cast<std::uint8_t*>(payload.data());
  std::size_t left = payload.size();
  while (left > 0) {
    ssize_t received = ::recv(fd, body, left, 0);
    if (received <= 0) {
      throw std::runtime_error("master server payload read failed");
    }
    body += received;
    left -= static_cast<std::size_t>(received);
  }
  return payload;
}

void MasterServer::WriteFrame(int fd, const std::string& payload) {
  std::uint32_t length = static_cast<std::uint32_t>(payload.size());
  auto* header = reinterpret_cast<const std::uint8_t*>(&length);
  std::size_t header_left = sizeof(length);
  while (header_left > 0) {
    ssize_t written = ::send(fd, header, header_left, 0);
    if (written <= 0) {
      throw std::runtime_error("master server header write failed");
    }
    header += written;
    header_left -= static_cast<std::size_t>(written);
  }

  auto* body = reinterpret_cast<const std::uint8_t*>(payload.data());
  std::size_t left = payload.size();
  while (left > 0) {
    ssize_t written = ::send(fd, body, left, 0);
    if (written <= 0) {
      throw std::runtime_error("master server payload write failed");
    }
    body += written;
    left -= static_cast<std::size_t>(written);
  }
}

}  // namespace nano_mooncake
