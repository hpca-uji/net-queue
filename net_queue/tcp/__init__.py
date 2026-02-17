"""TCP package"""

import ssl
import socket
import warnings
import selectors
from collections import abc
from concurrent import futures
from queue import Empty, SimpleQueue

from net_queue.utils import asynctools
from net_queue.core.comm import Communicator
from net_queue.core import CommunicatorOptions
from net_queue.utils.asynctools import thread_func


__all__ = (
    "Protocol",
)


type Task = abc.Callable[[], None]


# Sentinel objects
CONTROL_STOP = object()
CONTROL_EVENT = b"\0"


class Protocol(Communicator[socket.socket]):
    """Shared base TCP implementation"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Initialize communicator"""
        super().__init__(options)

        self._selector = selectors.DefaultSelector()
        self._control_socket = socket.socketpair()
        self._control_socket[1].setblocking(False)
        self._selector.register(self._control_socket[0], selectors.EVENT_READ, self._handle_control_socket)

        self._loop_thread = thread_func(self._handle_selector_loop)
        self._loop_thread.add_done_callback(asynctools.future_warn_exception)
        self._task_queue = SimpleQueue[Task]()

        # Receive fast-path
        if self.options.security:
            try:
                from sabctools import unlocked_ssl_recv_into
            except Exception:
                self._socket_recv_into = ssl.SSLSocket.recv_into
            else:
                self._socket_recv_into = unlocked_ssl_recv_into
        else:
            self._socket_recv_into = socket.socket.recv_into

    def _modify_selector(self, fileobj, events) -> None:
        """Modify registered events"""
        try:
            key = self._selector.get_key(fileobj)
        except ValueError:
            return  # already removed

        def callback():
            try:
                self._selector.modify(key.fd, events, key.data)
            except KeyError:
                return  # already removed

        self._task_queue.put(callback)

    def _notify_selector(self) -> None:
        """Interrupt selector loop"""
        try:
            self._control_socket[1].send(CONTROL_EVENT)
        except BlockingIOError:
            pass  # already notified

    def _handle_control_socket(self, sock: socket.socket, mask):
        """Handle selector notification"""
        if len(sock.recv(self.options.connection.transport_size)) == 0:
            return CONTROL_STOP

        # Handle tasks
        while True:
            try:
                task = self._task_queue.get_nowait()
            except Empty:
                break
            else:
                task()

    def _handle_selector_event(self, event: tuple[selectors.SelectorKey, int]):
        """Handle selector event"""
        key, mask = event
        callback, fileobj = key.data, key.fileobj
        return callback(fileobj, mask)

    def _handle_selector_loop(self) -> None:
        """Handle selector loop"""
        running = True
        while running:
            fs = [
                self._pool.submit(self._handle_selector_event, event)
                for event in self._selector.select()
            ]

            for future in futures.as_completed(fs):
                try:
                    result = future.result()
                except Exception as exc:
                    from traceback import format_exception
                    warnings.warn("\n".join(format_exception(exc)), RuntimeWarning)
                else:
                    if result is CONTROL_STOP:
                        running = False

    def _handle_recv(self, comm: socket.socket) -> None:
        """Receive communication"""
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        if session._get_buffer is not None:
            try:
                size = self._socket_recv_into(comm, session._get_buffer)  # type: ignore (SSL typing)
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                return
            else:
                with session._get_buffer:
                    session._get_buffer = session._get_buffer[size:]
                session.get_optimize()
        else:
            try:
                data = comm.recv(self.options.connection.transport_size)
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                return
            else:
                size = len(data)
                session.get_write(data)

        if self.options.security and (pending := comm.pending()):  # type: ignore (SSL typing)
            data = comm.recv(pending)
            session.get_write(data)

        if not size:
            if session.state or not session.put_empty():
                warnings.warn(f"Lost connection unexpectedly ({comm})", RuntimeWarning)
            return

        self._process_gets(peer)
        peer = session.peer

    def _handle_send(self, comm: socket.socket) -> None:
        """Send communication"""
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        size = 0
        session.put_flush_queue()
        if session._put_stream.empty():
            return
        with session.put_read() as view:
            try:
                size = comm.send(view)
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            if size < len(view):
                session._put_stream.unreadchunk(view[size:])
        self._put_commit(peer, size)

    def _close(self) -> None:
        """Close the communication"""
        self._control_socket[1].close()
        self._loop_thread.result()
        self._control_socket[0].close()
        self._selector.close()
        super()._close()
