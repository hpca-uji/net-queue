"""Communications package"""

# FIXME: session_ini/fin export future or surface error

import abc
import uuid
import enum
import typing
import warnings
import importlib
import threading
import dataclasses
from pathlib import Path
from typing import NamedTuple
from dataclasses import dataclass
from queue import Empty, SimpleQueue
from collections import abc as col_abc, deque
from concurrent.futures import Future, ThreadPoolExecutor

from bidict import bidict

from net_queue import asynctools
from net_queue.asynctools import merge_futures
from net_queue.asynctools import thread_queue
from net_queue.stream import Packer, PickleSerializer, Stream, byteview, bytearray


__all__ = (
    "NetworkLocation",
    "ConnectionOptions",
    "CommunicatorOptions",
    "SerializationOptions",
    "Protocol",
    "Purpose",
    "Message",
    "ResourceClosed",
    "Communicator",
    "Session",
    "new"
)


class ResourceClosed(RuntimeError):
    """Resource closed"""


class Protocol(enum.StrEnum):
    """Communication protocol"""
    GRPC = enum.auto()
    MQTT = enum.auto()
    TCP = enum.auto()


class Purpose(enum.StrEnum):
    """Communication purpose"""
    SERVER = enum.auto()
    CLIENT = enum.auto()


@dataclass(slots=True, frozen=True)
class Message[T]:
    """Message object"""
    peer: uuid.UUID
    data: T


class ConnectionStatus(enum.Flag):
    """Connection status"""
    READABLE = enum.auto()
    WRITABLE = enum.auto()


class NetworkLocation(NamedTuple):
    """Network location"""
    host: str = "127.0.0.1"
    port: int = 51966

    def __str__(self):
        """Unified network location"""
        return f"{self.host}:{self.port}"


@dataclass(order=False, slots=True, frozen=True)
class ConnectionOptions:
    """
    Connection options

    There are no definite values, depends on: use case, OS/stack/version and link specs.
    Its recommended to test your configuration (or preferably set them dynamically).
    There are *rules of thumb* but they serve as a baseline.
    """

    get_merge: bool = True
    put_merge: bool = True
    efficient_size: int = 64 * 1024 ** 1
    protocol_size: int = 4 * 1024 ** 2
    queue_size: int = 1 * 1024 ** 3
    message_size: int = 1 * 1024 ** 4


@dataclass(order=False, slots=True, frozen=True)
class SerializationOptions:
    """Serialization options"""
    load: col_abc.Callable[[Stream], typing.Any] = PickleSerializer().load
    dump: col_abc.Callable[[typing.Any], Stream] = PickleSerializer().dump


@dataclass(order=False, slots=True, frozen=True)
class SecurityOptions:
    """Security options"""
    key: Path | None = None
    certificate: Path | None = None


@dataclass(order=False, slots=True, frozen=True)
class CommunicatorOptions:
    """Communicator options"""
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    netloc: NetworkLocation = NetworkLocation()
    connection: ConnectionOptions = ConnectionOptions()
    serialization: SerializationOptions = SerializationOptions()
    security: SecurityOptions | None = None
    workers: int = 1


class Session:
    """Session data"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Initialize session data"""
        self.status = ConnectionStatus(value=0)
        self._options = options
        self.peer = options.id

        # Helpers
        self._packer = Packer()

        self._ack_queue = dict[Stream, Future]()
        self._ack_stream = deque[tuple[int, Future]]()

        # Get
        self._get_queue = SimpleQueue[Stream]()
        self._get_buffer: memoryview | None = None
        self._get_size = 0
        self._get_stream = Stream()

        if not self._options.connection.get_merge:
            self.get_optimize = lambda: None

        # Put
        self._put_queue = SimpleQueue[Stream]()
        self._put_buffer = byteview(bytearray(self._options.connection.protocol_size))
        self._put_size = min(self._options.connection.protocol_size, self._options.connection.efficient_size)
        self._put_stream = Stream()

        if not self._options.connection.put_merge:
            self.put_optimize = lambda: None

    def __repr__(self) -> str:
        """Session representation"""
        return f"<{self.__class__.__name__}" \
            f" peer={self.peer!r}" \
            f" status={self.status!r}" \
            f" ack-queue={len(self._ack_queue)!r}" \
            f" ack-stream={len(self._ack_stream)!r}" \
            f" get-queue={self._get_queue.qsize()!r}" \
            f" get-buffer={None if self._get_buffer is None else len(self._get_buffer)!r}" \
            f" get-size={self._get_size!r}" \
            f" get-stream={self._put_stream.nbytes!r}" \
            f" put-queue={self._put_queue.qsize()!r}" \
            f" put-buffer={None if self._put_buffer is None else len(self._put_buffer)!r}" \
            f" put-size={self._put_size!r}" \
            f" put-stream={self._put_stream.nbytes!r}" \
            ">"

    def get_empty(self) -> bool:
        """Is get connection flushed"""
        return self._get_queue.empty() and self._get_stream.empty() and self._get_buffer is None

    def put_empty(self) -> bool:
        """Is put connection flushed"""
        return self._put_queue.empty() and self._put_stream.empty()

    def get(self) -> Stream:
        """Unpack from get buffer"""
        return self._packer.unpack(self._get_stream, self._options.connection.message_size)

    def put(self, stream: Stream) -> Future[None]:
        """Push stream to put queue"""
        future = Future[None]()
        asynctools.future_set_running(future)
        self._ack_queue[stream] = future
        self._put_queue.put(stream)
        return future

    def get_write(self, b: col_abc.Buffer) -> int:
        """Write get buffer"""
        size = self._get_stream.write(b)
        self.get_optimize()
        return size

    def put_read(self) -> memoryview:
        """Read put buffer"""
        self.put_optimize()
        b = self._put_stream.read1(self._options.connection.protocol_size)
        return b

    def get_optimize(self) -> None:
        """Optimize get buffer"""

        # Generate buffer
        if self._get_buffer is None and not self._get_stream.empty():
            try:
                size = self._packer.unpack_header(self._get_stream)
            except BlockingIOError:
                pass
            else:
                self._get_size = size
                size = min(size, self._options.connection.message_size)
                if len(self._get_stream.peekchunk()) >= size:
                    size = 0
                self._get_buffer = byteview(bytearray(size))

        # Transfer to buffer
        if self._get_buffer is not None and not self._get_stream.empty() and len(self._get_buffer) > 0:
            size = self._get_stream.readinto(self._get_buffer)
            with self._get_buffer:
                self._get_buffer = self._get_buffer[size:]

        # Buffer full
        if self._get_buffer is not None and not len(self._get_buffer) > 0:
            with Stream() as stream:
                with self._get_buffer:
                    self._packer.pack_header(stream, self._get_size)
                    stream.write(self._get_buffer.obj)
                    self._get_buffer = None
                chunks = list(stream.readchunks())

            for chunk in reversed(chunks):
                self._get_stream.unreadchunk(chunk)

    def put_optimize(self) -> None:
        """Optimize put buffer"""
        if self._put_stream.nchunks <= 1 or len(self._put_stream.peekchunk()) >= self._put_size:
            return

        merge_size = self._put_stream.readinto(self._put_buffer)
        self._put_stream.unreadchunk(self._put_buffer[:merge_size])

    def put_commit(self, size: int) -> col_abc.Iterable[Future[None]]:
        """Mark size's bytes as fully transmitted (or freeable)"""
        while size > 0:
            pending, future = self._ack_stream[0]
            shared = min(size, pending)
            pending -= shared
            size -= shared

            assert size >= 0, "Committed more puts than queued"

            if pending > 0:
                self._ack_stream[0] = (pending, future)
                break

            self._ack_stream.popleft()
            yield future

    def get_flush_buffer(self) -> col_abc.Iterable[Stream]:
        """Flush get buffer"""
        try:
            while True:
                yield self.get()
        except BlockingIOError:
            pass

    def put_flush_queue(self) -> None:
        """Flush put queue"""
        try:
            while True:
                stream = self._put_queue.get_nowait()
                future = self._ack_queue.pop(stream)
                size = self._packer.pack(self._put_stream, stream)
                self._ack_stream.append((size, future))
        except Empty:
            pass

    def put_flush_buffer(self) -> col_abc.Iterable[memoryview]:
        """Flush put buffer"""
        try:
            while True:
                yield self.put_read()
        except BlockingIOError:
            pass


class Communicator[T](abc.ABC):
    """
    Communicator implementation

    Operations are thread-safe.

    Communicator has `with` support.
    """

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Communicator initialization"""
        super().__init__()
        self.options = options

        self._close_init = threading.Lock()
        self._close_done = threading.Event()

        self._lock = threading.Condition()
        self._get_events = SimpleQueue[uuid.UUID]()

        self._comms = bidict[uuid.UUID, T]()
        self._states = dict[uuid.UUID, Session]()

        thread_prefix = f"{__name__}.{self.__class__.__qualname__}:{id(self)}"

        self._put_queue = thread_queue(f"{thread_prefix}.put")
        self._pool = ThreadPoolExecutor(max_workers=self.options.workers, thread_name_prefix=f"{thread_prefix}.worker")

    def __repr__(self) -> str:
        """Communicator representation"""
        return f"{self.__class__.__name__}(options={self.options!r})"

    @property
    def id(self) -> uuid.UUID:
        """Communicator identifier"""
        return self.options.id

    def _session_ini(self, peer: uuid.UUID) -> None:
        """Send session ini message"""
        state = self._states[peer]
        if ConnectionStatus.WRITABLE in state.status:
            warnings.warn("Sending session ini on writable stream", RuntimeWarning)
        state.status |= ConnectionStatus.WRITABLE
        stream = Stream.frombytes(self.id.bytes)
        self._put(stream, peer)

    def _session_fin(self, peer: uuid.UUID) -> None:
        """Send session fin message"""
        state = self._states[peer]
        if ConnectionStatus.WRITABLE not in state.status:
            warnings.warn("Sending session fin on unwritable stream", RuntimeWarning)
        state.status &= ~ConnectionStatus.WRITABLE
        stream = Stream.frombytes(self.id.bytes)
        self._put(stream, peer)

    def _handle_session_ini(self, peer: uuid.UUID, id: uuid.UUID) -> None:
        """Handle session initialize message"""
        state = self._states[peer]
        if ConnectionStatus.READABLE in state.status:
            warnings.warn("Received session ini on readable stream", RuntimeWarning)

        comm = self._comms[peer]

        state.peer = id
        state.status |= ConnectionStatus.READABLE

        with self._lock:
            # New ID, move state from tmp ID
            if id not in self._comms:
                self._states[id] = state = self._states.pop(peer)

            # Old ID, reconnection, drop tmp ID
            elif peer != id:
                # FIXME: Transfer queues over
                del self._states[peer]

            # Update communicator ID association
            self._comms.inverse[comm] = id

    def _handle_session_fin(self, peer: uuid.UUID, id: uuid.UUID) -> None:
        """Handle session finalize message"""
        state = self._states[peer]
        if ConnectionStatus.READABLE not in state.status:
            warnings.warn("Received session fin on unreadable stream", RuntimeWarning)
        state.status &= ~ConnectionStatus.READABLE

    def _process_gets(self, peer: uuid.UUID) -> None:
        """Handle pending get packets"""
        state = self._states[peer]
        id_size = len(self.id.bytes)

        for stream in state.get_flush_buffer():

            # Handshake-like
            if stream.nbytes == id_size:
                chunk = stream.read()
                id = uuid.UUID(bytes=chunk.tobytes())

                # Handshake INI
                if state.peer == self.id:
                    self._handle_session_ini(peer, id)
                    peer = state.peer
                    continue

                # Handshake FIN
                elif state.peer == id:
                    self._handle_session_fin(peer, id)
                    peer = state.peer
                    continue

                # Data (handshake-like)
                else:
                    stream.writechunk(chunk)

            # Queue limits
            if state._get_queue.qsize() < self.options.connection.queue_size:
                state._get_queue.put(stream)
                self._get_events.put(peer)
            else:
                warnings.warn(f"Dropping data for {peer} (queue full)")

    def _put_commit(self, peer: uuid.UUID, size: int) -> None:
        """Commit size bytes as transmitted and resolve callbacks"""
        state = self._states[peer]

        for future in state.put_commit(size):
            self._put_queue.submit(asynctools.future_set_result, future, None).add_done_callback(asynctools.future_warn_exception)

    def _set_default_peer(self, comm: T) -> uuid.UUID:
        """Get associated peer or create a new one if missing"""
        try:
            return self._comms.inverse[comm]
        except KeyError:
            return self._connection_ini(comm)

    def _connection_post_ini(self, peer: uuid.UUID) -> None:
        """Additional connection ini after peer setup"""

    def _connection_pre_fin(self, peer: uuid.UUID) -> None:
        """Additional connection fin before peer teardown"""

    def _connection_ini(self, comm: T) -> uuid.UUID:
        """Handle new incomming connections"""
        # NOTE: communication thead
        peer = uuid.uuid4()  # temporary ID

        with self._lock:
            self._comms[peer] = comm
            self._states[peer] = Session(self.options)
            self._connection_post_ini(peer)

            # ACK
            peer = self._comms.inverse[comm]
            self._session_ini(peer)
            self._lock.notify_all()

        return peer

    def _connection_fin(self, comm: T) -> None:
        """Close connection"""
        with self._lock:
            peer = self._comms.inverse[comm]

            self._connection_pre_fin(peer)

            del self._comms[peer]

            # TODO: reuse _session_cleanup
            if self._states[peer].get_empty():
                del self._states[peer]

            self._lock.notify_all()

    def _session_cleanup(self, peer: uuid.UUID) -> None:
        """Remove finalized drained peer"""
        state = self._states[peer]

        if peer not in self._comms and state.get_empty():
            with self._lock:
                if peer not in self._comms and state.get_empty():
                    del self._states[peer]

    def _get(self, *peers: uuid.UUID) -> uuid.UUID:
        """Wait for a message from peer group, return which peer is ready"""
        return self._get_events.get()

    def get(self, *peers: uuid.UUID) -> Message:
        """
        Get data from peers

        Always block.
        Returns a message or raises ResourceClosed.
        Once closed it continues working until exhausted then it raises ResourceClosed.
        If no peers are defined, data is returned from the first available peer.

        Note: Currently peers can not be specified.
        """
        # NOTE: peers could be missing or disconnect creating infinite wait, which is an expected state during startup
        assert len(peers) == 0, "Communicators can not get from specific peer"
        peer = self._get(*peers)

        # Exit signaled
        if peer == self.id:
            raise ResourceClosed(self.id)

        state = self._states[peer]
        get_queue = state._get_queue

        # Get object
        stream = get_queue.get_nowait()

        self._session_cleanup(peer)

        with stream:
            data = self.options.serialization.load(stream)

        return Message(peer=peer, data=data)

    def _put(self, stream: Stream, peer: uuid.UUID) -> Future[None]:
        """Put stream into state"""
        try:
            state = self._states[peer]
        except KeyError:
            state = None
        if state:
            future = state.put(stream)
        else:
            future = Future[None]()
            asynctools.future_set_exception(future, ResourceClosed(peer))
        if self._closed:
            asynctools.future_set_exception(future, ResourceClosed(self.id))
        return future

    def put(self, data, *peers: uuid.UUID) -> Future[None]:
        """
        Publish data to peers

        For clients if no peers are defined, data is send to the server.
        For servers if no peers are defined, data is send to all clients.

        It is preferred to specify multiple peers instead of issuing multiple puts,
        as data will only be serialized once and protocols may use optimized routes.

        Note: Only servers can send to a particular client.

        Future is resolved when data is safe to mutate again.
        Future may raise `ResourceClose(uuid.UUID)` if the peer or itself are closed.
        Future may raise protocol specific exceptions.
        """
        if not peers:
            with self._lock:
                peers = tuple(self._comms)

        futures = list[Future[None]]()
        with self.options.serialization.dump(data) as stream:
            for peer in peers:
                future = self._put(stream.copy(), peer)
                futures.append(future)

        return merge_futures(futures)

    @property
    def _closed(self):
        """Is communicator closed"""
        return self._close_init.locked()

    def _close(self) -> None:
        """Communicator finalizer"""
        # Unlock inflight external API:
        for _ in range(threading.active_count()):
            self._get_events.put(self.id)
        self._pool.shutdown()
        self._put_queue.shutdown()

    def close(self) -> None:
        """Close the communicator"""
        if self._close_init.acquire(blocking=False):
            self._close()
            self._close_done.set()
        self._close_done.wait()

    def __enter__(self):
        """Context manager start"""
        return self

    def __exit__(self, cls, exc, tb):
        """Context manager exit"""
        self.close()

    def __del__(self) -> None:
        """Best effort finalizer"""
        try:
            self.close()
        except:  # noqa: E722
            pass


def new(protocol: Protocol = Protocol.TCP, purpose: Purpose = Purpose.CLIENT, options: CommunicatorOptions = CommunicatorOptions()) -> Communicator:
    """Generate communicator"""
    module = importlib.import_module(f"{__name__}.{protocol}.{purpose}")
    cls: type[Communicator] = getattr(module, "Communicator")
    return cls(options)
