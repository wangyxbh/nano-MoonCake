class NanoMooncakeClient:
    """Thin Python wrapper for the C++ Engine."""

    def __init__(self, engine):
        self._engine = engine

    def init(self, local_addr: str) -> None:
        self._engine.init(local_addr)

    def send(self, payload: bytes, peer_addr: str, key: str) -> None:
        self._engine.send_bytes(payload, peer_addr, key)

    def recv(self, key: str) -> bytes:
        return self._engine.recv_bytes(key)

    def close(self) -> None:
        self._engine.close()
