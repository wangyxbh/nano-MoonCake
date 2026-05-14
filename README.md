# nano-MoonCake

Minimal scaffold for a CUDA + TCP/RDMA learning project.

## Directory Skeleton

```text
nano-MoonCake/
  CMakeLists.txt
  include/
    nano_mooncake/
      engine.h
  csrc/
    nano_mooncake/
      engine.cpp
    python/
      bindings.cpp
  python/
    nano_mooncake/
      __init__.py
      client.py
  examples/
    tcp_cuda_demo.py
```

## Phase-1 Goal

- C++: implement `Engine::init/send/recv/close` with TCP first.
- CUDA: add device buffer handling (start with copy-to-host strategy).
- Python: expose stable API through pybind + thin wrapper.

## Current TCP Backend Notes

- `Engine` now boots with a minimal `TcpTransportBackend` by default.
- `Transport` is the reusable base class for shared transport state and submit
  flow; `TcpTransportBackend` only implements TCP-specific lifecycle and I/O.
- `start()` expects a TCP endpoint such as `tcp://127.0.0.1:9001`.
- `register_buffer()` and `mount_segment()` are separate, matching Mooncake's
  register-local-memory then mount-segment flow.
- `open_segment()` takes a logical `segment_name` plus a peer transport endpoint.
- The TCP backend serves mounted segments by name instead of exposing an
  implicit "first buffer".
- Remote metadata is resolved during `open_segment()`, so out-of-range requests
  fail before submit.

## Editable Install

```bash
pip install -e .
```

After installation:

```bash
python examples/tcp_cuda_demo.py
```
