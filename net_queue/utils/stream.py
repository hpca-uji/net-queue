"""Stream structure"""
from __future__ import annotations

import io
from collections import abc, deque

# bytearray fast-path
try:
    from sabctools import bytearray_malloc as bytearray
except Exception:
    pass

__all__ = (
    "byteview",
    "bytearray",
    "buffer_index",
    "Stream"
)


def byteview(b: abc.Buffer) -> memoryview:
    """Return a byte view of a buffer"""
    with memoryview(b) as view:
        return view.cast("B")


def buffer_index(b: abc.Buffer, sub: bytes) -> int:
    """Find lowest index where substring is found"""
    if len(sub) != 1:
        raise TypeError("Only single byte substring are supported")
    for i, byte in enumerate(b):  # type: ignore
        if byte == sub:
            return i
    else:
        raise ValueError("Substring not found")


class Stream(io.BufferedIOBase):
    """
    Zero-copy non-blocking pipe-like

    Interface mimics a non-blocking BufferedRandom,
    but operations return memoryviews instead of bytes.

    Operations are not thread-safe.
    Reader is responsible of releasing chunks.
    Writer hands off responsibility over chunks.

    Stream has `with` and `bytes` support.
    Stream has `copy()` support, however it does not support `deepcopy()`.
    """

    __slots__ = ("_nbytes", "_chunks")

    def __init__(self):
        """Initialize stream"""
        self._nbytes = 0
        self._chunks = deque[memoryview]()

    # stream properties
    @property
    def nchunks(self) -> int:
        """Number of chunks held in stream"""
        return len(self._chunks)

    @property
    def nbytes(self) -> int:
        """Number of bytes held in stream"""
        return self._nbytes

    def empty(self) -> bool:
        """Is stream empty (would read block)"""
        return not bool(self._chunks)

    # stream base methods
    def unreadchunk(self, chunk: memoryview) -> int:
        """Unread a chunk into the stream"""
        size = len(chunk)

        if size > 0:
            self._chunks.appendleft(chunk)
        else:
            chunk.release()

        self._nbytes += size
        return size

    def readchunk(self) -> memoryview:
        """Read a chunk from stream"""
        if self.empty():
            raise BlockingIOError()

        chunk = self._chunks.popleft()
        self._nbytes -= len(chunk)
        return chunk

    def unwritechunk(self) -> memoryview:
        """Unwrite a chunk from the stream"""
        if self.empty():
            raise BlockingIOError()

        chunk = self._chunks.pop()
        self._nbytes -= len(chunk)
        return chunk

    def writechunk(self, chunk: memoryview) -> int:
        """Write a chunk into the stream"""
        size = len(chunk)

        if size > 0:
            self._chunks.append(chunk)
        else:
            chunk.release()

        self._nbytes += size
        return size

    def peekchunk(self) -> memoryview:
        """Peek a chunk from stream"""
        if self.empty():
            return byteview(b"")

        return self._chunks[0]

    # stream extended methods
    def readchunks(self) -> abc.Iterable[memoryview]:
        """Read all chunks from stream"""
        while not self.empty():
            yield self.readchunk()

    def writechunks(self, chunks: abc.Iterable[memoryview]) -> int:
        """Write many chunks into the stream"""
        size = 0

        for chunk in chunks:
            size += self.writechunk(chunk)

        return size

    def update(self, bs: abc.Iterable[abc.Buffer]) -> int:
        """Write many buffers into the stream"""
        size = 0

        for b in bs:
            size += self.write(b)

        return size

    def clear(self) -> None:
        """Release all chunks"""
        for chunk in self.readchunks():
            chunk.release()

    def copy(self) -> Stream:
        """Shallow copy of stream"""
        other = self.__class__()
        other.update(self._chunks)
        return other

    def tobytes(self) -> bytes:
        """Transform stream to bytes (will copy)"""
        if self.empty():
            return b""
        return self.read().tobytes()

    @classmethod
    def frombytes(cls, b: abc.Buffer) -> Stream:
        """Construct a stream from bytes"""
        stream = cls()
        stream.write(b)
        return stream

    def __bytes__(self) -> bytes:
        """Transform stream to bytes (will copy)"""
        return self.tobytes()

    def __copy__(self) -> Stream:
        """Shallow copy of stream"""
        return self.copy()

    # io methods
    def write(self, b: abc.Buffer) -> int:
        """Inserts buffer into stream"""
        chunk = byteview(b)
        size = self.writechunk(chunk)
        return size

    def readline(self) -> memoryview:
        """Read a line and return a memoryview (may copy)"""
        if self.empty():
            raise BlockingIOError()

        with Stream() as stream:
            for chunk in self.readchunks():
                try:
                    i = buffer_index(chunk, b"\n")
                except ValueError:
                    stream.writechunk(chunk)
                    continue
                else:
                    self.unreadchunk(chunk)
                    chunk = self.read1(i)
                    stream.writechunk(chunk)
                    break
            return stream.read()

    def read1(self, size: int = -1, /) -> memoryview:
        """Reads, with at most one operation, and returns a memoryview"""
        chunk = self.readchunk()

        if size < 0 or size >= len(chunk):
            return chunk

        with chunk:
            read, keep = chunk[:size], chunk[size:]
        self.unreadchunk(keep)
        return read

    def readinto1(self, b: abc.Buffer, /) -> int:
        """Reads, with at most one operation, into a buffer"""
        with byteview(b) as view:
            with self.read1(len(view)) as chunk:
                size = len(chunk)
                view[:size] = chunk

        return size

    def readinto(self, b: abc.Buffer, /) -> int:
        """Reads, until drained, into a buffer"""
        if self.empty():
            raise BlockingIOError()

        read = 0
        with byteview(b) as view:
            while not self.empty() and read < len(view):
                with view[read:] as subview:
                    read += self.readinto1(subview)

        return read

    def read(self, size: int = -1, /) -> memoryview:
        """Reads, until drained, and returns a memoryview (may copy)"""
        if size < 0:
            size = self.nbytes
        else:
            size = min(size, self.nbytes)

        # View path
        chunk = self.read1(size)
        if len(chunk) == size:
            return chunk

        # Copy path
        buffer = bytearray(size)
        self.unreadchunk(chunk)
        self.readinto(buffer)
        return byteview(buffer)

    def __del__(self) -> None:
        """Best effort finalizer"""
        try:
            self.close()
        except:  # noqa: E722
            pass

    # io stubs
    def __iter__(self) -> abc.Iterable[memoryview]:
        return self.readlines()

    def fileno(self) -> int:
        raise OSError()

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def readlines(self):
        while not self.empty():
            yield self.readline()

    def seek(self, offset, whence=io.SEEK_SET, /) -> int:
        raise OSError()

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise OSError()

    def truncate(self, size=None, /) -> int:
        raise OSError()

    def writable(self) -> bool:
        return True

    def writelines(self, lines: abc.Iterable[abc.Buffer], /) -> None:
        self.update(lines)

    def detach(self) -> abc.Buffer:
        raise io.UnsupportedOperation()

    def close(self) -> None:
        self.clear()
