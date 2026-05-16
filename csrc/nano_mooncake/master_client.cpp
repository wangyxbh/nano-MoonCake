#include "nano_mooncake/master_client.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace nano_mooncake {

namespace {

struct Endpoint {
  std::string host;
  std::uint16_t port = 0;
};

Endpoint ParseEndpoint(const std::string& endpoint) {
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

void ReadFully(int fd, void* data, std::size_t bytes) {
  auto* cursor = static_cast<std::uint8_t*>(data);
  while (bytes > 0) {
    ssize_t received = ::recv(fd, cursor, bytes, 0);
    if (received <= 0) {
      throw std::runtime_error("master socket read failed");
    }
    cursor += received;
    bytes -= static_cast<std::size_t>(received);
  }
}

void WriteFully(int fd, const void* data, std::size_t bytes) {
  auto* cursor = static_cast<const std::uint8_t*>(data);
  while (bytes > 0) {
    ssize_t written = ::send(fd, cursor, bytes, 0);
    if (written <= 0) {
      throw std::runtime_error("master socket write failed");
    }
    cursor += written;
    bytes -= static_cast<std::size_t>(written);
  }
}

int Connect(const std::string& endpoint) {
  auto parsed = ParseEndpoint(endpoint);
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create master client socket");
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(parsed.port);
  if (::inet_pton(AF_INET, parsed.host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::invalid_argument("master host must be an IPv4 address");
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    throw std::runtime_error("failed to connect to mooncake_master");
  }
  return fd;
}

void WriteFrame(int fd, const std::string& payload) {
  std::uint32_t length = static_cast<std::uint32_t>(payload.size());
  WriteFully(fd, &length, sizeof(length));
  if (!payload.empty()) {
    WriteFully(fd, payload.data(), payload.size());
  }
}

std::string ReadFrame(int fd) {
  std::uint32_t length = 0;
  ReadFully(fd, &length, sizeof(length));
  std::string payload(length, '\0');
  if (length > 0) {
    ReadFully(fd, payload.data(), length);
  }
  return payload;
}

}  // namespace

MasterClient::MasterClient(std::string master_endpoint)
    : master_endpoint_(std::move(master_endpoint)) {}

void MasterClient::MountSegment(const MasterSegmentRecord& segment) {
  MasterRequest request{
      .opcode = MasterOpcode::kMountSegment,
      .segment_name = segment.segment_name,
      .segment = segment,
  };
  auto response = RoundTrip(request);
  if (!response.ok) {
    throw std::runtime_error(response.message);
  }
}

void MasterClient::UnmountSegment(const std::string& segment_name) {
  MasterRequest request{
      .opcode = MasterOpcode::kUnmountSegment,
      .segment_name = segment_name,
  };
  auto response = RoundTrip(request);
  if (!response.ok) {
    throw std::runtime_error(response.message);
  }
}

std::optional<MasterSegmentRecord> MasterClient::ResolveSegment(
    const std::string& segment_name) {
  MasterRequest request{
      .opcode = MasterOpcode::kResolveSegment,
      .segment_name = segment_name,
  };
  auto response = RoundTrip(request);
  if (!response.ok) {
    return std::nullopt;
  }
  return response.segment;
}

void MasterClient::PutObject(const ObjectLocationRecord& object) {
  MasterRequest request{
      .opcode = MasterOpcode::kPutObject,
      .segment_name = object.segment_name,
      .object_key = object.key,
      .object = object,
  };
  auto response = RoundTrip(request);
  if (!response.ok) {
    throw std::runtime_error(response.message);
  }
}

std::optional<ObjectLocationRecord> MasterClient::GetObject(
    const std::string& key) {
  MasterRequest request{
      .opcode = MasterOpcode::kGetObject,
      .object_key = key,
  };
  auto response = RoundTrip(request);
  if (!response.ok) {
    return std::nullopt;
  }
  return response.object;
}

std::vector<MasterSegmentRecord> MasterClient::ListSegments() {
  auto response = RoundTrip(MasterRequest{.opcode = MasterOpcode::kListSegments});
  if (!response.ok) {
    throw std::runtime_error(response.message);
  }
  return response.segments;
}

std::vector<ObjectLocationRecord> MasterClient::ListObjects() {
  auto response = RoundTrip(MasterRequest{.opcode = MasterOpcode::kListObjects});
  if (!response.ok) {
    throw std::runtime_error(response.message);
  }
  return response.objects;
}

MasterResponse MasterClient::RoundTrip(const MasterRequest& request) {
  int fd = Connect(master_endpoint_);
  try {
    WriteFrame(fd, SerializeMasterRequest(request));
    auto payload = ReadFrame(fd);
    ::close(fd);
    return ParseMasterResponse(payload);
  } catch (...) {
    ::close(fd);
    throw;
  }
}

}  // namespace nano_mooncake
