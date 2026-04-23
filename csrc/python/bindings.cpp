#include <pybind11/pybind11.h>

#include <string>

#include "nano_mooncake/engine.h"

namespace py = pybind11;

PYBIND11_MODULE(_nano_mooncake, m) {

  py::enum_<nano_mooncake::DeviceType>(m, "DeviceType")
      .value("CPU", nano_mooncake::DeviceType::kCPU)
      .value("CUDA", nano_mooncake::DeviceType::kCUDA);

  py::enum_<nano_mooncake::TransferOpcode>(m, "TransferOpcode")
      .value("READ", nano_mooncake::TransferOpcode::kRead)
      .value("WRITE", nano_mooncake::TransferOpcode::kWrite);

  py::enum_<nano_mooncake::TransferState>(m, "TransferState")
      .value("PENDING", nano_mooncake::TransferState::kPending)
      .value("IN_FLIGHT", nano_mooncake::TransferState::kInFlight)
      .value("COMPLETED", nano_mooncake::TransferState::kCompleted)
      .value("FAILED", nano_mooncake::TransferState::kFailed);

  py::class_<nano_mooncake::RemoteSegmentHandle>(m, "RemoteSegmentHandle")
      .def(py::init<>())
      .def_readwrite("segment_id", &nano_mooncake::RemoteSegmentHandle::segment_id)
      .def_readwrite("segment_name",
                     &nano_mooncake::RemoteSegmentHandle::segment_name)
      .def_readwrite("peer_endpoint",
                     &nano_mooncake::RemoteSegmentHandle::peer_endpoint)
      .def_readwrite("remote_base_addr",
                     &nano_mooncake::RemoteSegmentHandle::remote_base_addr);

  py::class_<nano_mooncake::RemoteBufferRef>(m, "RemoteBufferRef")
      .def(py::init<>())
      .def_readwrite("segment", &nano_mooncake::RemoteBufferRef::segment)
      .def_readwrite("offset", &nano_mooncake::RemoteBufferRef::offset)
      .def_readwrite("length", &nano_mooncake::RemoteBufferRef::length);

  py::class_<nano_mooncake::RegisteredBuffer>(m, "RegisteredBuffer")
      .def(py::init<>())
      .def_readwrite("buffer_id", &nano_mooncake::RegisteredBuffer::buffer_id)
      .def_readwrite("location", &nano_mooncake::RegisteredBuffer::location)
      .def_readwrite("remote_accessible",
                     &nano_mooncake::RegisteredBuffer::remote_accessible);

  py::class_<nano_mooncake::BatchHandle>(m, "BatchHandle")
      .def(py::init<>())
      .def_readwrite("batch_id", &nano_mooncake::BatchHandle::batch_id)
      .def_readwrite("capacity", &nano_mooncake::BatchHandle::capacity);

  py::class_<nano_mooncake::TransferStatus>(m, "TransferStatus")
      .def(py::init<>())
      .def_readwrite("state", &nano_mooncake::TransferStatus::state)
      .def_readwrite("transferred_bytes",
                     &nano_mooncake::TransferStatus::transferred_bytes)
      .def_readwrite("message", &nano_mooncake::TransferStatus::message)
      .def_readwrite("error_code", &nano_mooncake::TransferStatus::error_code);

  py::class_<nano_mooncake::Engine>(m, "Engine")
      .def(py::init<>())
      .def("start", &nano_mooncake::Engine::start, py::arg("local_addr"))
      .def("init", &nano_mooncake::Engine::init, py::arg("local_addr"))
      .def(
          "register_buffer",
          [](nano_mooncake::Engine& self, std::uintptr_t addr, std::size_t bytes,
             const std::string& location, bool remote_accessible,
             nano_mooncake::DeviceType device) {
            nano_mooncake::BufferView view{
                .data = reinterpret_cast<void*>(addr),
                .bytes = bytes,
                .device = device,
            };
            return self.register_buffer(view, location, remote_accessible);
          },
          py::arg("addr"), py::arg("bytes"), py::arg("location") = "any",
          py::arg("remote_accessible") = true,
          py::arg("device") = nano_mooncake::DeviceType::kCPU)
      .def("open_segment", &nano_mooncake::Engine::open_segment,
           py::arg("segment_name"))
      .def("allocate_batch", &nano_mooncake::Engine::allocate_batch,
           py::arg("capacity"))
      .def("submit_write", &nano_mooncake::Engine::submit_write,
           py::arg("local_buffer_id"), py::arg("remote"))
      .def("submit_read", &nano_mooncake::Engine::submit_read, py::arg("remote"),
           py::arg("local_buffer_id"))
      .def("poll", &nano_mooncake::Engine::poll, py::arg("batch_id"))
      .def("wait", &nano_mooncake::Engine::wait, py::arg("batch_id"),
           py::arg("timeout_ms") = -1)
      .def("stop", &nano_mooncake::Engine::stop)
      .def("close", &nano_mooncake::Engine::close);
}
