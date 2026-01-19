"""Communications package"""

from queue import Empty, SimpleQueue
from concurrent.futures import Future
from collections import abc as col_abc, deque

from net_queue.utils import asynctools
from net_queue.utils.stream import Stream, byteview, bytearray
from net_queue.core import CommunicatorOptions, SessionState
from net_queue.utils.streamtools import Packer

__all__ = (
    "Session",
)


class Session:
    """Session data"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Initialize session data"""
        self.state = SessionState(value=0)
        self._options = options
        self.peer = options.id

        # Helpers
        self._packer = Packer()

        self._ack_queue = dict[Stream, Future]()
        self._ack_stream = deque[tuple[int, Future]]()

        # Get
        self._get_queue = SimpleQueue[Stream]()
        self._get_stream = Stream()

        if self._options.connection.get_merge:
            self._get_buffer: memoryview | None = None
            self._get_size = 0
        else:
            self._get_buffer = None
            self.get_optimize = lambda: None

        # Put
        self._put_queue = SimpleQueue[Stream]()
        self._put_stream = Stream()

        if self._options.connection.put_merge:
            self._put_buffer = byteview(bytearray(self._options.connection.protocol_size))
            self._put_size = min(self._options.connection.protocol_size, self._options.connection.efficient_size)
        else:
            self._put_buffer = None
            self.put_optimize = lambda: None

    def __repr__(self) -> str:
        """Session representation"""
        return f"<{self.__class__.__name__}" \
            f" peer={self.peer!r}" \
            f" status={self.state!r}" \
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

        assert self._put_buffer is not None, "Put buffer missing!"
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
