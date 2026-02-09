"""TCP client"""

import ssl
import uuid
import copy
import socket
import selectors
from concurrent.futures import Future

from net_queue.core import client
from net_queue.socket_tcp import Transport
from net_queue.utils.stream import Stream
from net_queue.core.comm import CommunicatorOptions


__all__ = (
    "Communicator",
)


class Communicator(Transport, client.Client[socket.socket]):
    """TCP client"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Client initialization"""
        super().__init__(copy.replace(options, workers=1))

        # TCP
        self._comm = socket.create_connection(self.options.netloc)

        if self.options.security:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=self.options.security.certificate)
            self._comm = context.wrap_socket(self._comm, server_hostname=self.options.netloc.host)

        self._comm.setblocking(False)

        self._selector.register(self._comm, selectors.EVENT_READ, self._handle_connection)

        self._connection_ini(self._comm)

    def _handle_connection(self, comm: socket.socket, event) -> None:
        """Handle connection events"""
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        if session.put_empty():
            self._modify_selector(comm, selectors.EVENT_READ)

        if event & selectors.EVENT_WRITE:
            self._handle_send(comm)

        if event & selectors.EVENT_READ:
            self._handle_recv(comm)

        if not session.put_empty():
            self._modify_selector(comm, selectors.EVENT_READ | selectors.EVENT_WRITE)

        self._notify_selector()

        if not session.state and session.put_empty():
            self._connection_fin(comm)

    def _connection_pre_fin(self, peer: uuid.UUID) -> None:
        """Close connection"""
        self._selector.unregister(self._comm)
        self._comm.close()
        del self._comm
        super()._connection_pre_fin(peer)

    def _put(self, stream: Stream, peer: uuid.UUID) -> Future[None]:
        """Put stream into queue and notify"""
        future = super()._put(stream, peer)
        self._modify_selector(self._comm, selectors.EVENT_READ | selectors.EVENT_WRITE)
        self._notify_selector()
        return future

    def _close(self) -> None:
        """Close the client"""
        comm = self._comm
        peer = self._comms.inverse[comm]
        self._session_fin(peer)

        with self._lock:
            while hasattr(self, "_comm"):
                self._lock.wait()

        super()._close()
