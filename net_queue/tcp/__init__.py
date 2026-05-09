"""TCP package"""

import os
import ssl
import socket
import warnings
import selectors
import itertools
from collections import abc
from concurrent import futures
from queue import Empty, SimpleQueue
from traceback import format_exception

from net_queue.core.comm import Communicator
from net_queue.core import CommunicatorOptions
from net_queue.core.session import Session
from net_queue.utils.futures import background, warn_exception
from streamview import Stream


__all__ = (
    "Protocol",
)


type Task = abc.Callable[[], None]


# Constants
try:
    SC_IOV_MAX = os.sysconf("SC_IOV_MAX")
except Exception:
    SC_IOV_MAX = None


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

        self._loop_thread = background(self._handle_selector_loop)
        self._loop_thread.add_done_callback(warn_exception)
        self._task_queue = SimpleQueue[Task]()

        # Send fast-path
        if not self.options.security and getattr(socket.socket, "sendmsg"):
            self._socket_send = self._socket_send_batch

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
                    warnings.warn("".join(format_exception(exc)), RuntimeWarning)
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

    @staticmethod
    def _socket_send(comm: socket.socket, session: Session) -> int:
        """Send an optimally-sized chunk over socket"""
        session.put_optimize()
        with session._put_stream[0] as view:
            try:
                size = comm.send(view)
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            return session._put_stream.seek(size)

    @staticmethod
    def _socket_send_batch(comm: socket.socket, session: Session) -> int:
        """Send a chunked batch over socket"""
        views = itertools.islice(session._put_stream.chunks, SC_IOV_MAX)
        size = comm.sendmsg(views)
        return session._put_stream.seek(size)

    def _handle_send(self, comm: socket.socket) -> None:
        """Send communication"""
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        size = 0
        session.put_flush_queue()
        if not session._put_stream:
            return
        size = self._socket_send(comm, session)
        self._put_commit(peer, size)

    def _close(self) -> None:
        """Close the communication"""
        self._control_socket[1].close()
        self._loop_thread.result()
        self._control_socket[0].close()
        self._selector.close()
        super()._close()
