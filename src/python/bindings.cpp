#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <stdexcept>
#include <string>

#include "nano_mooncake/engine.h"

namespace py = pybind11;

PYBIND11_MODULE(_nano_mooncake, m) {
  m.doc() = "Minimal nano Mooncake binding";

  py::class_<nano_mooncake::Engine>(m, "Engine")
      .def(py::init<>())
      .def("init", &nano_mooncake::Engine::init, py::arg("local_addr"))
      .def(
          "send_bytes",
          [](nano_mooncake::Engine& self, py::bytes payload,
             const std::string& peer_addr, const std::string& key) {
            std::string data = payload;
            nano_mooncake::BufferView view{
                .data = data.data(),
                .bytes = data.size(),
                .device = nano_mooncake::DeviceType::kCPU,
            };
            self.send(view, peer_addr, key);
          },
          py::arg("payload"), py::arg("peer_addr"), py::arg("key"))
      .def(
          "recv_bytes",
          [](nano_mooncake::Engine& self, const std::string& key) {
            auto out = self.recv(key);
            return py::bytes(reinterpret_cast<const char*>(out.data()),
                             static_cast<py::ssize_t>(out.size()));
          },
          py::arg("key"))
      .def("close", &nano_mooncake::Engine::close);
}
