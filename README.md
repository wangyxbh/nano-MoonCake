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

## Editable Install

```bash
pip install -e .
```

After installation:

```bash
python examples/tcp_cuda_demo.py
```
