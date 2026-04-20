#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace nano_mooncake {

enum class DeviceType : uint8_t {
  kCPU = 0,
  kCUDA = 1,
};

struct BufferView {
  void* data = nullptr;
  std::size_t bytes = 0;
  DeviceType device = DeviceType::kCPU;
};

class Engine {
 public:
  Engine() = default;
  ~Engine() = default;

  void init(const std::string& local_addr);
  void send(const BufferView& buffer, const std::string& peer_addr,
            const std::string& key);
  std::vector<std::uint8_t> recv(const std::string& key);
  void close();

 private:
  std::string local_addr_;
  bool initialized_ = false;
};

}  // namespace nano_mooncake
