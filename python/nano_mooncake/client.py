class NanoMooncakeClient:
    """Minimal client wrapper for nano-MoonCake control and data planes."""

    def __init__(self, engine):
        self._engine = engine

    def start(self, local_addr: str, master_addr: str, client_id: str) -> None:
        self._engine.start(local_addr, master_addr, client_id)

    def init(self, local_addr: str, master_addr: str, client_id: str) -> None:
        self.start(local_addr, master_addr, client_id)

    def register_buffer(
        self,
        addr: int,
        bytes_len: int,
        location: str = "any",
        remote_accessible: bool = True,
        device=None,
    ):
        if device is None:
            return self._engine.register_buffer(
                addr, bytes_len, location, remote_accessible
            )
        return self._engine.register_buffer(
            addr, bytes_len, location, remote_accessible, device
        )

    def unregister_buffer(self, buffer_id: int):
        return self._engine.unregister_buffer(buffer_id)

    def mount_segment(
        self, segment_name: str, buffer_id: int, transport_endpoint: str = ""
    ):
        return self._engine.mount_segment(segment_name, buffer_id, transport_endpoint)

    def unmount_segment(self, segment_name: str):
        return self._engine.unmount_segment(segment_name)

    def unsegment(self, segment_name: str):
        return self._engine.unsegment(segment_name)

    def resolve_segment(self, segment_name: str):
        return self._engine.resolve_segment(segment_name)

    def put_object(
        self, key: str, segment_name: str, offset: int, length: int
    ) -> None:
        self._engine.put_object(key, segment_name, offset, length)

    def get_object(self, key: str):
        return self._engine.get_object(key)

    def open_segment(self, segment_name: str):
        return self._engine.open_segment(segment_name)

    def close_segment(self, segment_id: int):
        return self._engine.close_segment(segment_id)

    def submit_write(self, local_buffer_id: int, remote):
        return self._engine.submit_write(local_buffer_id, remote)

    def submit_read(self, remote, local_buffer_id: int):
        return self._engine.submit_read(remote, local_buffer_id)

    def read_object(self, key: str, local_buffer_id: int):
        return self._engine.read_object(key, local_buffer_id)

    def poll(self, batch_id: int):
        return self._engine.poll(batch_id)

    def wait(self, batch_id: int, timeout_ms: int = -1):
        return self._engine.wait(batch_id, timeout_ms)

    def stop(self) -> None:
        self._engine.stop()

    def close(self) -> None:
        # Backward-compatible alias for older call sites.
        self.stop()
