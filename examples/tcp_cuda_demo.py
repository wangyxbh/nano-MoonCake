import ctypes
import traceback


def _status_name(status_obj) -> str:
    name = str(status_obj.state)
    if "." in name:
        return name.split(".")[-1]
    return name


def main() -> None:
    try:
        from nano_mooncake import _nano_mooncake as nm
    except Exception:
        print("FAIL: cannot import _nano_mooncake")
        print("Hint: build with -DNANO_BUILD_PYTHON=ON and set PYTHONPATH.")
        print(traceback.format_exc())
        return

    print("== nano-MoonCake stage0 minimal demo ==")
    engine = nm.Engine()

    # Use bytearray so we can get a stable writable pointer.
    host_buf = bytearray(b"hello-mooncake-stage0")
    addr = ctypes.addressof(ctypes.c_char.from_buffer(host_buf))
    size = len(host_buf)

    try:
        print("[1/6] start")
        engine.start("local://demo")

        print("[2/6] register_buffer")
        local = engine.register_buffer(addr, size, "cpu:0", True)
        print(f"  local.buffer_id={local.buffer_id}, size={size}")

        print("[3/6] open_segment")
        seg = engine.open_segment("peer-demo-segment")
        print(f"  segment_id={seg.segment_id}, name={seg.segment_name}")

        print("[4/6] build remote ref")
        remote = nm.RemoteBufferRef()
        remote.segment = seg
        remote.offset = 0
        remote.length = size

        print("[5/6] submit_write + poll/wait")
        batch = engine.submit_write(local.buffer_id, remote)
        polled = engine.poll(batch.batch_id)
        waited = engine.wait(batch.batch_id, 1000)
        print(
            f"  poll={_status_name(polled)} bytes={polled.transferred_bytes}, "
            f"wait={_status_name(waited)} bytes={waited.transferred_bytes}"
        )
        if _status_name(waited) != "COMPLETED":
            raise RuntimeError(
                f"batch not completed: state={_status_name(waited)} "
                f"msg={waited.message} code={waited.error_code}"
            )

        print("[6/6] stop")
        engine.stop()
        print("PASS: start/register/open/submit/poll/wait/stop all succeeded.")
    except Exception:
        print("FAIL: runtime error in demo flow")
        print(traceback.format_exc())
        try:
            engine.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
