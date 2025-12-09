"""Communications package"""

# FIXME: session_ini/fin export future or surface error
# FIXME: TCP / GRPC threading preformance
# FIXME: TCP + TLS preformance

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
from net_queue.stream import Packer, PickleSerializer, Stream, byteview


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
    """Comunication protocol"""
    GRPC = enum.auto()
    MQTT = enum.auto()
    TCP = enum.auto()


class Purpose(enum.StrEnum):
    """Comunication purpose"""
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

    There are no definite values, depends on: usecase, OS/stack/version and link specs.
    Its recommended to test your configuration (or preferably set them dynamicly).
    There are *ruls of thumb* but they serve as a baseline.
    """

    max_size: int
    merge_size: int
    efficient_size: int

    def __init__(self, max_size: int = 0, merge_size: int = 0, efficient_size: int = 0):
        """Inizialize connection options"""
        # NOTE: Frozen dataclasess must use object.__setattr__ during __init__
        object.__setattr__(self, "max_size", max_size if max_size else 4 * 1024 ** 2)
        object.__setattr__(self, "merge_size", merge_size if merge_size else self.max_size)
        object.__setattr__(self, "efficient_size", efficient_size if efficient_size else self.max_size // 64)


@dataclass(order=False, slots=True, frozen=True)
class SerializationOptions:
    """Serialization options"""
    queue_size: int = -1
    message_size: int = -1
    load: col_abc.Callable[[Stream], typing.Any] = PickleSerializer().load
    dump: col_abc.Callable[[typing.Any], Stream] = PickleSerializer().dump


@dataclass(order=False, slots=True, frozen=True)
class SecurityOptions:
    """Security options"""
    key: Path | None = None
    certificate: Path | None = None


@dataclass(order=False, slots=True, frozen=True)
class CommunicatorOptions:
    """Comunicatior options"""
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    netloc: NetworkLocation = NetworkLocation()
    workers: int = 1
    connection: ConnectionOptions = ConnectionOptions()
    serialization: SerializationOptions = SerializationOptions()
    security: SecurityOptions | None = None


class Session:
    """Session data"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Initialize session data"""
        self.peer = options.id
        self._options = options
        self.status = ConnectionStatus(value=0)

        self._packer = Packer()
        self._merge_buffer = byteview(bytearray(self._options.connection.merge_size))
        self._merge_size = min(self._options.connection.merge_size, self._options.connection.efficient_size)

        self.put_queue = SimpleQueue[Stream]()
        self.get_queue = SimpleQueue[Stream]()
        self.put_buffer = Stream()
        self.get_buffer = Stream()

        self._ack_queue = dict[Stream, Future]()
        self._ack_buffer = deque[tuple[int, Future]]()

    def get_empty(self) -> bool:
        """Is get connection flushed"""
        return self.get_queue.empty() and self.get_buffer.empty()

    def put_empty(self) -> bool:
        """Is put connection flushed"""
        return self.put_queue.empty() and self.put_buffer.empty()

    def get(self) -> Stream:
        """Unpack from get buffer"""
        return self._packer.unpack(self.get_buffer, self._options.serialization.message_size)

    def put(self, stream: Stream) -> Future[None]:
        """Push stream to put queue"""
        future = Future[None]()
        asynctools.future_set_running(future)
        self._ack_queue[stream] = future
        self.put_queue.put(stream)
        return future

    def get_write(self, b: col_abc.Buffer) -> int:
        """Write get buffer (merging chunks if plausible)"""
        size = self.get_buffer.write(b)

        if self.get_buffer.nchunks > 1 and size < self._merge_size:
            self._get_merge()

        return size

    def _get_merge(self) -> None:
        """Merge get buffer"""
        with Stream() as stream:

            while not self.get_buffer.empty():
                chunk = self.get_buffer.unwritechunk()

                if len(chunk) >= self._merge_size:
                    self.get_buffer.writechunk(chunk)
                    break

                stream.unreadchunk(chunk)

            if stream.nbytes < self._merge_size:
                self.get_buffer.writechunks(stream.readchunks())
                return

            while not stream.empty():
                chunk = stream.read(self._options.connection.merge_size)
                self.get_buffer.writechunk(chunk)

    def get_flush_buffer(self) -> col_abc.Iterable[Stream]:
        """Flush get buffer"""
        try:
            while True:
                yield self.get()
        except BlockingIOError:
            pass

    def _put_merge(self) -> None:
        """Merge put buffer"""
        merge_size = self.put_buffer.readinto(self._merge_buffer)
        self.put_buffer.unreadchunk(self._merge_buffer[:merge_size])

    def put_read(self) -> memoryview:
        """Read put buffer (merging chunks if plausible)"""
        if self.put_buffer.nchunks > 1 and len(self.put_buffer.peekchunk()) < self._merge_size:
            self._put_merge()

        return self.put_buffer.read1(self._options.connection.max_size)

    def put_commit(self, size: int) -> col_abc.Iterable[Future[None]]:
        """Mark size's bytes as fully transmitted (or freeable)"""
        while size > 0:
            pending, future = self._ack_buffer[0]
            shared = min(size, pending)
            pending -= shared
            size -= shared

            assert size >= 0, "Commited more puts than queued"

            if pending > 0:
                self._ack_buffer[0] = (pending, future)
                break

            self._ack_buffer.popleft()
            yield future

    def put_flush_queue(self) -> None:
        """Flush put queue"""
        while True:
            try:
                stream = self.put_queue.get_nowait()
            except Empty:
                break
            else:
                future = self._ack_queue.pop(stream)
                size = self._packer.pack(self.put_buffer, stream)
                self._ack_buffer.append((size, future))

    def put_flush_buffer(self) -> col_abc.Iterable[memoryview]:
        """Flush put buffer"""
        while not self.put_buffer.empty():
            yield self.put_read()


class Communicator[T](abc.ABC):
    """
    Communicator implementation

    Operations are thread-safe.

    Comunicator has `with` support.
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

        self._pool = ThreadPoolExecutor(max_workers=self.options.workers, thread_name_prefix=f"{thread_prefix}.worker")
        self._put_queue = thread_queue(f"{thread_prefix}.put")

    def __repr__(self) -> str:
        """Comunicator representation"""
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
            warnings.warn("Recived session ini on readable stream", RuntimeWarning)

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
            warnings.warn("Recived session fin on unreadable stream", RuntimeWarning)
        state.status &= ~ConnectionStatus.READABLE

    def _process_gets(self, peer: uuid.UUID) -> None:
        """Handle pending get packets"""
        state = self._states[peer]
        id_size = len(self.id.bytes)
        queue_size = self.options.serialization.queue_size

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
            if queue_size < 0 or state.get_queue.qsize() < queue_size:
                state.get_queue.put(stream)
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
        """Additional connection fin before peer taredown"""

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
        assert len(peers) == 0, "Comunicators can not get from specific peer"
        peer = self._get(*peers)

        # Exit signaled
        if peer == self.id:
            raise ResourceClosed(self.id)

        state = self._states[peer]
        get_queue = state.get_queue

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

        It is prefered to specify multiple peers insted of issuing multiple puts,
        as data will only be serialized once and protocols may use optimized routes.

        Note: Only servers can send to a particular client.

        Future is resolved when data is safe to mutate again.
        Future may raise `ResouceClose(uuid.UUID)` if the peer or itself are closed.
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
    """Generate comunicator"""
    module = importlib.import_module(f"{__name__}.{protocol}.{purpose}")
    cls: type[Communicator] = getattr(module, "Communicator")
    return cls(options)
