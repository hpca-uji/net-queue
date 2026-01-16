"""Communications package"""

# FIXME: session_ini/fin export future or surface error

import abc
import uuid
import warnings
import threading
from queue import SimpleQueue
from concurrent.futures import Future, ThreadPoolExecutor

from bidict import bidict

from net_queue.utils import asynctools
from net_queue.utils.asynctools import merge_futures
from net_queue.utils.asynctools import thread_queue
from net_queue.utils.stream import Stream
from net_queue.core import CommunicatorOptions, SessionState, Message
from net_queue.core.session import Session


__all__ = (
    "Communicator",
)


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
        self._sessions = dict[uuid.UUID, Session]()

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
        session = self._sessions[peer]
        if SessionState.WRITABLE in session.state:
            warnings.warn("Sending session ini on writable stream", RuntimeWarning)
        session.state |= SessionState.WRITABLE
        stream = Stream.frombytes(self.id.bytes)
        self._put(stream, peer)

    def _session_fin(self, peer: uuid.UUID) -> None:
        """Send session fin message"""
        session = self._sessions[peer]
        if SessionState.WRITABLE not in session.state:
            warnings.warn("Sending session fin on unwritable stream", RuntimeWarning)
        session.state &= ~SessionState.WRITABLE
        stream = Stream.frombytes(self.id.bytes)
        self._put(stream, peer)

    def _handle_session_ini(self, peer: uuid.UUID, id: uuid.UUID) -> None:
        """Handle session initialize message"""
        session = self._sessions[peer]
        if SessionState.READABLE in session.state:
            warnings.warn("Received session ini on readable stream", RuntimeWarning)

        comm = self._comms[peer]

        session.peer = id
        session.state |= SessionState.READABLE

        with self._lock:
            # New ID, move session from tmp ID
            if id not in self._comms:
                self._sessions[id] = session = self._sessions.pop(peer)

            # Old ID, reconnection, drop tmp ID
            elif peer != id:
                # FIXME: Transfer queues over
                del self._sessions[peer]

            # Update communicator ID association
            self._comms.inverse[comm] = id

    def _handle_session_fin(self, peer: uuid.UUID, id: uuid.UUID) -> None:
        """Handle session finalize message"""
        session = self._sessions[peer]
        if SessionState.READABLE not in session.state:
            warnings.warn("Received session fin on unreadable stream", RuntimeWarning)
        session.state &= ~SessionState.READABLE

    def _process_gets(self, peer: uuid.UUID) -> None:
        """Handle pending get packets"""
        session = self._sessions[peer]
        id_size = len(self.id.bytes)

        for stream in session.get_flush_buffer():

            # Handshake-like
            if stream.nbytes == id_size:
                chunk = stream.read()
                id = uuid.UUID(bytes=chunk.tobytes())

                # Handshake INI
                if session.peer == self.id:
                    self._handle_session_ini(peer, id)
                    peer = session.peer
                    continue

                # Handshake FIN
                elif session.peer == id:
                    self._handle_session_fin(peer, id)
                    peer = session.peer
                    continue

                # Data (handshake-like)
                else:
                    stream.writechunk(chunk)

            # Queue limits
            if session._get_queue.qsize() < self.options.connection.queue_size:
                session._get_queue.put(stream)
                self._get_events.put(peer)
            else:
                warnings.warn(f"Dropping data for {peer} (queue full)")

    def _put_commit(self, peer: uuid.UUID, size: int) -> None:
        """Commit size bytes as transmitted and resolve callbacks"""
        session = self._sessions[peer]

        for future in session.put_commit(size):
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
            self._sessions[peer] = Session(self.options)
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
            if self._sessions[peer].get_empty():
                del self._sessions[peer]

            self._lock.notify_all()

    def _session_cleanup(self, peer: uuid.UUID) -> None:
        """Remove finalized drained peer"""
        session = self._sessions[peer]

        if peer not in self._comms and session.get_empty():
            with self._lock:
                if peer not in self._comms and session.get_empty():
                    del self._sessions[peer]

    def _get(self, *peers: uuid.UUID) -> uuid.UUID:
        """Wait for a message from peer group, return which peer is ready"""
        return self._get_events.get()

    def get(self, *peers: uuid.UUID) -> Message:
        """
        Get data from peers

        Always block.
        Returns a message or raises ConnectionError.
        Once closed it continues working until exhausted then it raises ConnectionError.
        If no peers are defined, data is returned from the first available peer.

        Note: Currently peers can not be specified.
        """
        # NOTE: peers could be missing or disconnect creating infinite wait, which is an expected state during startup
        assert len(peers) == 0, "Communicators can not get from specific peer"
        peer = self._get(*peers)

        # Exit signaled
        if peer == self.id:
            raise ConnectionAbortedError(self.id)

        state = self._sessions[peer]
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
            session = self._sessions[peer]
        except KeyError:
            session = None
        if session:
            future = session.put(stream)
        else:
            future = Future[None]()
            asynctools.future_set_exception(future, ConnectionResetError(peer))
        if self._closed:
            asynctools.future_set_exception(future, ConnectionAbortedError(self.id))
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
        Future may raise `ConnectionError(uuid.UUID)` if the peer or itself are closed.
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
