class NanoMooncakeClient:
    """Stage-0 wrapper for transfer-engine style API."""

    def __init__(self, engine):
        self._engine = engine

    def init(self, local_addr: str) -> None:
        self._engine.init(local_addr)

    def register_buffer(
        self,
        addr: int,
        bytes_len: int,
        location: str = "any",
        remote_accessible: bool = True,
    ):
        return self._engine.register_buffer(
            addr, bytes_len, location, remote_accessible
        )

    def open_segment(self, segment_name: str):
        return self._engine.open_segment(segment_name)

    def submit_write(self, local_buffer_id: int, remote):
        return self._engine.submit_write(local_buffer_id, remote)

    def submit_read(self, remote, local_buffer_id: int):
        return self._engine.submit_read(remote, local_buffer_id)

    def poll(self, batch_id: int):
        return self._engine.poll(batch_id)

    def wait(self, batch_id: int, timeout_ms: int = -1):
        return self._engine.wait(batch_id, timeout_ms)

    def close(self) -> None:
        self._engine.close()
