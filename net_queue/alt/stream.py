"""Alternative stream structure"""

from collections import abc

from net_queue.utils.stream import Stream, byteview


__all__ = (
    "PipeStream",
    "BytesStream"
)


class PipeStream(Stream):
    """Stream that emulates a pipe behavior"""

    # stream extended methods
    def copy(self) -> Stream:
        """Shallow copy of stream"""
        other = self.__class__()
        other.update(map(bytes, self._chunks))
        return other

    # io methods
    def write(self, b: abc.Buffer) -> int:
        """Inserts buffer into stream"""
        return super().write(bytes(b))

    def read1(self, size: int = -1, /) -> memoryview:
        """Reads, with at most one operation, and returns a memoryview"""
        return byteview(bytes(super().read1(size)))


class BytesStream(Stream):
    """Stream that emulates a bytes behavior"""

    def _merge_chunks(self) -> None:
        """Merge chunks into contiguous memory"""
        chunk = byteview(b"".join(self._chunks))
        self._chunks.clear()
        self._chunks.append(chunk)

    # stream base methods
    def unreadchunk(self, chunk: memoryview) -> int:
        """Unread a chunk into the stream"""
        size = super().unreadchunk(chunk)
        self._merge_chunks()
        return size

    def writechunk(self, chunk: memoryview) -> int:
        """Write a chunk into the stream"""
        size = super().writechunk(chunk)
        self._merge_chunks()
        return size
