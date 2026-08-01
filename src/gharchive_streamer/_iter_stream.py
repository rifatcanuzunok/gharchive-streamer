import io
from collections.abc import Iterator


class IterStream(io.RawIOBase):
    def __init__(self, iterator: Iterator[bytes]) -> None:
        self._it = iterator
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self._buffer) < size:
            try:
                chunk = next(self._it)
                if not chunk:
                    continue
                self._buffer += chunk
            except StopIteration:
                break

        if size < 0:
            data, self._buffer = self._buffer, b""
        else:
            data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data
