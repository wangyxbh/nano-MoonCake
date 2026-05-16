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
- Control plane and data plane are both TCP-based in V1, but use separate protocols.

## Core APIs

- `Engine.start(local_addr, master_addr, client_id)`
- `register_buffer(...)`
- `mount_segment(segment_name, buffer_id)`
- `put_object(key, segment_name, offset, length)`
- `get_object(key)`
- `read_object(key, local_buffer_id)`

## Build

```bash
pip install -e .
```

The standalone master executable is built as `mooncake_master` when using CMake.

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
