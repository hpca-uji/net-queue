"""Stream utilities"""

import re
import pickle
import warnings
from io import BytesIO
from typing import Any
from struct import Struct
from collections import abc

from streamview import Stream, byteview


__all__ = (
    "Framer",
    "BytesSerializer",
    "PickleSerializer"
)


class Framer:
    """
    Stream framer

    Packs or unpacks streams into another.

    Operations are not thread-safe.
    """

    # NOTE: Format:
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


class _Unpickler(pickle.Unpickler):
    """Unpickler with find_class filter"""

    @staticmethod
    def allow_class(name: str) -> bool:
        """Is object deserialization allowed?"""
        return True

    def find_class(self, module: str, name: str):
        """Return an object from a specified module"""
        global_name = f"{module}.{name}"

        if self.allow_class(global_name):
            return super().find_class(module, name)

        raise pickle.UnpicklingError(f"{global_name} is forbidden")


class PickleSerializer:
    """Pickle-stream serializer"""

    __slots__ = ("_allow", "_dump", "_load")
    _protocol = max(5, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def allow_by_name(*names: str) -> abc.Callable[[str], bool]:
        """
        Generate an allow function filtering by global names

        Example: `["builtins"]` for a whole module
        Example: `["uuid.UUID"]` for a single class
        """
        return re.compile(rf"^({"|".join(map(re.escape, names))})(\.|$)").match  # type: ignore

    def __init__(self, allow: abc.Callable[[str], bool] | None = None, dump: abc.Callable[[Any], Any] | None = None, load: abc.Callable[[Any], Any] | None = None) -> None:
        """Initialize serializer"""
        self._allow = allow
        self._load = load
        self._dump = dump

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        stream = Stream()
        pickler = pickle.Pickler(file=stream, protocol=self._protocol)

        if self._dump is not None:
            pickler.persistent_id = self._dump

        pickler.dump(data)
        return stream

    def load(self, data: Stream):
        """Transform a stream into useful data"""
        if self._allow is not None:
            unpickler = _Unpickler(file=data)
            unpickler.allow_class = self._allow  # type: ignore
        else:
            unpickler = pickle.Unpickler(file=data)

        if self._load is not None:
            unpickler.persistent_load = self._load

        return unpickler.load()


class _PickleSerializer(PickleSerializer):
    """Serializer that emulates a pickle behavior"""

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        stream = BytesIO()
        pickler = pickle.Pickler(file=stream, protocol=self._protocol)

        if self._dump is not None:
            pickler.persistent_id = self._dump

        pickler.dump(data)
        return Stream.frombytes(stream.getvalue())
