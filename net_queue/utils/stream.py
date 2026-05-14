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
    "StreamFramer",
    "StreamSerializer",
    "BufferSerializer",
    "PickleSerializer"
)


class StreamFramer:
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
        read = self._header.size
        if transport.nbytes < read:
            raise BlockingIOError()

        # Read size
        with transport.read(read) as b:
            return self._header.unpack(b)[0]

    def pack_header(self, transport: Stream, size: int) -> int:
        """Inserts header into packer, returns bytes written"""
        header = self._header.pack(size)
        size = transport.write(header)
        return size

    def unpack(self, transport: Stream, size: int = -1) -> Stream:
        """Extracts stream from packer (raises BlockingIOError if no stream)"""
        # Check if size available
        read = self._header.size
        if transport.nbytes < read:
            raise BlockingIOError()

        # Read size
        view = transport.read(read)
        read = self._header.unpack(view)[0]

        # Compute limits
        if size >= 0 and size < read:
            extra = read - size
            read = size
        else:
            extra = 0

        # Check if data available
        if transport.nbytes < read:
            transport.unreadview(view)
            raise BlockingIOError()
        else:
            view.release()

        # Ensure contained reads
        upper = Stream()
        while read > 0:
            view = transport.read1(read)
            upper.writeview(view)
            read -= len(view)

        # Write truncated header
        if extra > 0:
            warnings.warn("Truncated stream!", ResourceWarning)
            header = self._header.pack(extra)
            transport.unreadview(byteview(header))

        # Return stream
        return upper

    def pack(self, transport: Stream, data: Stream) -> int:
        """Inserts stream into packer, returns bytes written"""
        # Ensure contained writes
        written = data.nbytes
        header = self._header.pack(written)
        written += transport.write(header)
        transport.writeviews(data.readviews())
        return written


class StreamSerializer:
    """Stream-passthrough serializer"""

    __slots__ = ()

    def load(self, data: Stream) -> Stream:
        """Passthrough stream as-is"""
        return data

    def dump(self, data: Stream) -> Stream:
        """Passthrough stream as view"""
        return data.copy()


class BufferSerializer:
    """Buffer-stream serializer"""

    __slots__ = ()

    def load(self, data: Stream) -> memoryview:
        """Transform a stream into a contiguous buffer (may copy)"""
        return data.toview()

    def dump(self, data: abc.Buffer) -> Stream:
        """Transform a buffer into a stream"""
        return Stream.frombuffer(data)


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

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        stream = Stream()
        pickler = pickle.Pickler(file=stream, protocol=self._protocol)

        if self._dump is not None:
            pickler.persistent_id = self._dump

        pickler.dump(data)
        return stream


class _PickleSerializer(PickleSerializer):
    """Serializer that emulates a pickle behavior"""

    def load(self, data: Stream):
        """Transform a stream into useful data"""
        stream = BytesIO(data.toview())
        return super().load(stream)  # type: ignore

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        stream = BytesIO()
        pickler = pickle.Pickler(file=stream, protocol=self._protocol)

        if self._dump is not None:
            pickler.persistent_id = self._dump

        pickler.dump(data)
        return Stream.frombuffer(stream.getvalue())
