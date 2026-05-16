#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "master_protocol.h"
#include "mooncake_master.h"

namespace nano_mooncake {

class MasterServer {
 public:
  MasterServer() = default;
  ~MasterServer();

  void Start(const std::string& listen_endpoint);
  void Stop();

 private:
  struct Endpoint {
    std::string host;
    std::uint16_t port = 0;
  };

  void AcceptLoop();
  void HandleClient(int client_fd);
  MasterResponse Dispatch(const MasterRequest& request);

  static Endpoint ParseEndpoint(const std::string& endpoint);
  static std::string ReadFrame(int fd);
  static void WriteFrame(int fd, const std::string& payload);

  mooncake_master service_;
  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
  std::thread accept_thread_;
};

}  // namespace nano_mooncake
