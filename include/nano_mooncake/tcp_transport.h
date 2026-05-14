#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "transport.h"

namespace nano_mooncake {

// TCP transport only owns socket protocol details.
// Buffer/segment/batch bookkeeping lives in the reusable Transport base class.
class TcpTransportBackend : public Transport {
 public:
  TcpTransportBackend() = default;
  ~TcpTransportBackend() override;

  std::string name() const override;
  TransportCapabilities capabilities() const override;

 private:
  void do_start(const std::string& local_endpoint) override;
  void do_stop() override;
  void do_prepare_segment(RemoteSegmentHandle& segment) override;
  void do_submit_request(const ResolvedTransferRequest& request) override;

  struct Endpoint {
    std::string host;
    std::uint16_t port = 0;
  };

  struct WireHeader {
    std::uint32_t magic = 0;
    std::uint16_t version = 0;
    std::uint16_t opcode = 0;
    std::uint16_t name_length = 0;
    std::uint16_t reserved = 0;
    std::uint64_t offset = 0;
    std::uint64_t length = 0;
  };

  struct DescribeReply {
    std::uint32_t status = 0;
    std::uint64_t remote_base_addr = 0;
    std::uint64_t remote_bytes = 0;
    char message[96] = {};
  };

  void accept_loop();
  void handle_client(int client_fd);
  Endpoint parse_endpoint(const std::string& endpoint) const;
  int connect_to(const Endpoint& endpoint) const;

  static void read_fully(int fd, void* data, std::size_t bytes);
  static void write_fully(int fd, const void* data, std::size_t bytes);
  static std::string trim_message(const char* raw, std::size_t bytes);

  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
  Endpoint local_endpoint_;
  std::thread accept_thread_;
};

}  // namespace nano_mooncake
