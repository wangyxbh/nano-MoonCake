#include "nano_mooncake/engine.h"

#include <stdexcept>

namespace nano_mooncake {

void Engine::init(const std::string& local_addr) {
  if (local_addr.empty()) {
    throw std::invalid_argument("local_addr must not be empty");
  }
  local_addr_ = local_addr;
  initialized_ = true;
}

void Engine::send(const BufferView& buffer, const std::string& peer_addr,
                  const std::string& key) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (buffer.data == nullptr || buffer.bytes == 0) {
    throw std::invalid_argument("buffer must not be empty");
  }
  if (peer_addr.empty() || key.empty()) {
    throw std::invalid_argument("peer_addr and key must not be empty");
  }

  // TODO(nano): implement TCP transport send path.
  // TODO(nano): add CUDA handling (device pointer validation and copy strategy).
}

std::vector<std::uint8_t> Engine::recv(const std::string& key) {
  if (!initialized_) {
    throw std::runtime_error("Engine is not initialized");
  }
  if (key.empty()) {
    throw std::invalid_argument("key must not be empty");
  }

  // TODO(nano): implement TCP transport receive path.
  return {};
}

void Engine::close() {
  initialized_ = false;
  local_addr_.clear();
}

}  // namespace nano_mooncake
