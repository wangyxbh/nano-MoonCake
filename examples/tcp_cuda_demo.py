import argparse
import ctypes
import time
import traceback


def _status_name(status_obj) -> str:
    name = str(status_obj.state)
    if "." in name:
        return name.split(".")[-1]
    return name


def _import_nm():
    from nano_mooncake import _nano_mooncake as nm

    return nm


def run_provider(args) -> None:
    nm = _import_nm()
    engine = nm.Engine()
    payload = bytearray(args.payload.encode("utf-8"))
    addr = ctypes.addressof(ctypes.c_char.from_buffer(payload))

    engine.start(args.local_addr, args.master_addr, args.client_id)
    local = engine.register_buffer(addr, len(payload), "cpu:0", True, nm.DeviceType.CPU)
    mounted = engine.mount_segment(args.segment_name, local.buffer_id)
    engine.put_object(args.key, mounted.segment_name, 0, len(payload))

    print(
        f"provider ready: key={args.key} segment={mounted.segment_name} "
        f"endpoint={mounted.transport_endpoint} bytes={len(payload)}"
    )
    try:
      while True:
          time.sleep(60)
    except KeyboardInterrupt:
      engine.stop()


def run_consumer(args) -> None:
    nm = _import_nm()
    engine = nm.Engine()
    host_buf = bytearray(args.buffer_size)
    addr = ctypes.addressof(ctypes.c_char.from_buffer(host_buf))

    engine.start(args.local_addr, args.master_addr, args.client_id)
    local = engine.register_buffer(addr, len(host_buf), "cpu:0", True, nm.DeviceType.CPU)
    obj = engine.get_object(args.key)
    if obj is None:
        raise RuntimeError(f"object {args.key} not found in mooncake_master")
    batch = engine.read_object(args.key, local.buffer_id)
    waited = engine.wait(batch.batch_id, 3000)
    if _status_name(waited) != "COMPLETED":
        raise RuntimeError(
            f"read failed: state={_status_name(waited)} msg={waited.message}"
        )
    data = bytes(host_buf[: obj.length]).decode("utf-8", errors="ignore")
    print(
        f"consumer fetched: key={args.key} segment={obj.segment_name} "
        f"offset={obj.offset} length={obj.length} data={data}"
    )
    engine.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="nano-MoonCake three-process demo")
    sub = parser.add_subparsers(dest="role", required=True)

    provider = sub.add_parser("provider")
    provider.add_argument("--local-addr", required=True)
    provider.add_argument("--master-addr", required=True)
    provider.add_argument("--client-id", required=True)
    provider.add_argument("--segment-name", required=True)
    provider.add_argument("--key", required=True)
    provider.add_argument("--payload", default="hello-mooncake")

    consumer = sub.add_parser("consumer")
    consumer.add_argument("--local-addr", required=True)
    consumer.add_argument("--master-addr", required=True)
    consumer.add_argument("--client-id", required=True)
    consumer.add_argument("--key", required=True)
    consumer.add_argument("--buffer-size", type=int, default=4096)

    args = parser.parse_args()
    try:
        if args.role == "provider":
            run_provider(args)
        else:
            run_consumer(args)
    except Exception:
        print("FAIL: demo runtime error")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
