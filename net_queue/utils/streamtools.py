"""Stream utilities"""

import pickle
import warnings
import functools
from struct import Struct
from collections import abc

from net_queue.utils.stream import Stream, byteview


__all__ = (
    "Packer",
    "BytesSerializer",
    "PickleSerializer"
)


class Packer:
    """
    Stream packer

    Packs or unpacks streams into another.
    Supports ancillary streams for control information.

    Operations are not thread-safe.
    """

    # NOTE: Packed format:
    # - Network byte order (big-endian)
    # - Size only includes stream (not itself)
    #
    # +---------------+-------------------+
    # | Size (uint64) | Stream (variable) |
    # +---------------+-------------------+

    # TODO: Use header methods for data variants
    # TODO: Use uintvar (VLQ) instead of uint64 in packer

    __slots__ = ()
    _header = Struct("!Q")

    def unpack_header(self, transport: Stream) -> int:
        """Extracts header from packer (raises BlockingIOError if no stream)"""
        # Check if size available
        rb = self._header.size
        if transport.nbytes < rb:
            raise BlockingIOError()

        # Read size
        with transport.read(rb) as chunk:
            return self._header.unpack(chunk)[0]

    def pack_header(self, transport: Stream, size: int) -> int:
        """Inserts header into packer, returns bytes written"""
        pack = self._header.pack(size)
        size = transport.write(pack)
        return size

    def unpack(self, transport: Stream, size: int = -1) -> Stream:
        """Extracts stream from packer (raises BlockingIOError if no stream)"""
        # Check if size available
        rb = self._header.size
        if transport.nbytes < rb:
            raise BlockingIOError()

        # Read size
        chunk = transport.read(rb)
        rb = self._header.unpack(chunk)[0]

        # Compute limits
        if size >= 0 and size < rb:
            extra = rb - size
            rb = size
        else:
            extra = 0

        # Check if data available
        if transport.nbytes < rb:
            transport.unreadchunk(chunk)
            raise BlockingIOError()
        else:
            chunk.release()

        # Ensure contained reads
        upper = Stream()
        while rb > 0:
            chunk = transport.read1(rb)
            upper.writechunk(chunk)
            rb -= len(chunk)

        # Write truncated header
        if extra > 0:
            warnings.warn("Truncated stream!", ResourceWarning)
            pack = self._header.pack(extra)
            transport.unreadchunk(byteview(pack))

        # Return stream
        return upper

    def pack(self, transport: Stream, data: Stream) -> int:
        """Inserts stream into packer, returns bytes written"""
        # Ensure contained writes
        wb = data.nbytes
        pack = self._header.pack(wb)
        wb += transport.write(pack)
        transport.writechunks(data.readchunks())
        return wb


class BytesSerializer:
    """Bytes-stream serializer"""

    __slots__ = ()

    def dump(self, data: bytes) -> Stream:
        """Transform a data into a stream"""
        return Stream.frombytes(data)

    def load(self, data: Stream) -> bytes:
        """Transform a stream into useful data"""
        return data.tobytes()


class PickleSerializer:
    """Pickle-stream serializer"""

    __slots__ = ("_dump", "_load")

    def __init__(self, restrict: abc.Iterable[str] | None = None) -> None:
        """Initialize serializer"""
        # Setup context
        self._dump = pickle.dump

        if restrict is None:
            self._load = pickle.load
            return
        else:
            restrict = frozenset(restrict)

        # Setup restricted context
        def allow_class(name: str) -> bool:
            r, s, _ = name, ".", ""
            while r:
                if r in restrict:
                    return True
                r, s, _ = r.rpartition(s)
            return False

        class Deserializer(pickle.Unpickler):
            def find_class(self, module: str, name: str):
                global_name = f"{module}.{name}"
                if allow_class(global_name):
                    return super().find_class(module, name)
                raise pickle.UnpicklingError(f"{global_name} is forbidden")

        @functools.wraps(pickle.load)
        def load(*args, **kwds):
            return Deserializer(*args, *kwds).load()

        self._load = load

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        stream = Stream()
        self._dump(obj=data, file=stream, protocol=5)
        return stream

    def load(self, data: Stream):
        """Transform a stream into useful data"""
        return self._load(data)
