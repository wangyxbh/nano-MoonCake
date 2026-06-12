# nano-MoonCake

Minimal scaffold for learning a Mooncake-like control plane + transfer engine split.

## Current Shape

- `mooncake_master` is now a standalone control-plane process.
- Each client process:
  - starts a local transport endpoint
  - registers and mounts local segments
  - publishes segment metadata to `mooncake_master`
  - publishes object metadata with `PutObject`
  - queries object metadata with `GetObject`
  - connects directly to the data owner for transfer
- Data transfer stays peer-to-peer through `Transport` / `TcpTransportBackend`.
- Control plane stays TCP-based. Data plane can now use either:
  - `tcp://host:port`
  - `rdma://host:port?device=mlx5_0&port_num=1&gid_index=0`

## Core APIs

- `Engine.start(local_addr, master_addr, client_id)`
- `register_buffer(...)`
- `mount_segment(segment_name, buffer_id)`
- `unsegment(segment_name)`
- `put_object(key, segment_name, offset, length)`
- `get_object(key)`
- `read_object(key, local_buffer_id)`

## Build

```bash
pip install -e .
```

The standalone master executable is built as `mooncake_master` when using CMake.

RDMA is optional at build time. If `libibverbs` headers and libraries are found,
`RdmaTransportBackend` is compiled in automatically; otherwise the build stays
TCP-only and any `rdma://...` endpoint will fail fast with a clear runtime error.

CUDA/GDR is also optional. If `cuda.h` and `libcuda` are available during build,
the RDMA backend can register `DeviceType::kCUDA` buffers for GPU Direct RDMA.

Useful RDMA knobs:

- `NANO_MOONCAKE_RDMA_DEVICE`: default RDMA device if not passed in endpoint query.
- `NANO_MOONCAKE_RDMA_GID_INDEX`: default GID index override.

Current RDMA scope:

- control plane is still `tcp://...`
- data plane handshake uses a small TCP socket embedded in the `rdma://...` endpoint
- transfer path uses verbs RDMA read/write
- CPU buffers use ordinary `ibv_reg_mr`
- CUDA buffers can use GPU Direct RDMA when CUDA driver support is built in:
  - `WITH_NVIDIA_PEERMEM=1` uses legacy `ibv_reg_mr(cuda_ptr)` and requires `nvidia-peermem`
  - `WITH_NVIDIA_PEERMEM=0` uses `ibv_reg_dmabuf_mr` via CUDA DMA-BUF export

## Three-Process Demo

Start master:

```bash
./build/mooncake_master tcp://127.0.0.1:19999
```

Start provider:

```bash
python examples/tcp_cuda_demo.py provider \
  --local-addr tcp://127.0.0.1:19001 \
  --master-addr tcp://127.0.0.1:19999 \
  --client-id provider-1 \
  --segment-name seg-a \
  --key object-a \
  --payload hello-mooncake
```

Start consumer:

```bash
python examples/tcp_cuda_demo.py consumer \
  --local-addr tcp://127.0.0.1:19002 \
  --master-addr tcp://127.0.0.1:19999 \
  --client-id consumer-1 \
  --key object-a
```

## Low-Overhead Trace Logging

Set `NANO_MOONCAKE_TRACE=1` to enable structured trace logs on `stderr`.
Tracing is off by default, and the hot path only pays the extra timestamp/logging cost
when this flag is enabled.

```bash
NANO_MOONCAKE_TRACE=1 python examples/tcp_cuda_demo.py consumer ...
```

Each log line is a single event:

```text
nm_trace ts_us=... trace_id=... component="engine" event="read_object" ...
```

Useful fields:

- `trace_id`: correlates one end-to-end flow across engine, master, and TCP peer.
- `component`: `engine`, `master_client`, `master_server`, `tcp_transport`, `tcp_transport_server`, `transport`.
- `event`: phase name such as `connect`, `write_request`, `read_response`, `prepare_segment`, `submit_request`, `handle_request`.
- `duration_us`: time spent in that phase.
- `bytes`: bytes carried by that phase.
- `request_id` / `batch_id`: data-plane correlation ids.

For a `read_object(...)` path, the main chain is typically:

1. `engine:get_object`
2. `master_client/master_server:get_object`
3. `engine:open_segment`
4. `master_client/master_server:resolve_segment`
5. `tcp_transport:prepare_segment`
6. `tcp_transport:submit_request`
7. `tcp_transport_server:handle_request`
8. `transport:submit_batch`
9. `engine:submit_request`
10. `engine:read_object`

With RDMA enabled, the data-plane stages become:

5. `rdma_transport:prepare_segment_connect`
6. `rdma_transport:prepare_segment`
7. `transport:submit_batch`
8. `rdma_transport:submit_request`
9. `engine:submit_request`
