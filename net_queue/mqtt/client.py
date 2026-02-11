"""MQTT client"""

import uuid
from concurrent.futures import Future

import paho.mqtt.client as mqtt_client

from net_queue.core import client
from net_queue.utils import asynctools
from net_queue.mqtt import Protocol
from net_queue.utils.stream import Stream
from net_queue.core.comm import CommunicatorOptions


__all__ = (
    "Communicator",
)


# Sentinel objects
END_COMM = b""


class Communicator(Protocol, client.Client[str]):
    """MQTT client"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Client initialization"""
        super().__init__(options)

        # MQTT
        self._comm = self.id.hex
        self._register_handler(topic=f"s2c/{self._comm}", handler=self._handle_message)

        self._connection_ini(self._comm)

    def _handle_message(self, client: mqtt_client.Client, userdata, message: mqtt_client.MQTTMessage) -> None:
        """Broker message handler"""
        comm = self._peer(message)
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]
        session.get_write(message.payload)
        self._process_gets(peer)
        peer = session.peer

        if not session.state and session.put_empty():
            self._connection_fin(comm)

    def _put(self, stream: Stream, peer: uuid.UUID) -> Future[None]:
        """Put stream into queue and notify"""
        future = super()._put(stream, peer)
        self._pool.submit(self._c2s, self._comm).add_done_callback(asynctools.future_warn_exception)
        return future

    def _c2s(self, comm: str):
        """Client to server communication"""
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        size = 0
        session.put_flush_queue()
        for view in session.put_flush_buffer():
            with view:
                self._publish(f"c2s/{self.id.hex}", bytes(view))
                size += len(view)
        self._put_commit(peer, size)

    def _close(self) -> None:
        """Close the client"""
        comm = self._comm
        peer = self._comms.inverse[comm]
        self._session_fin(peer)

        # Request loop thread to stop
        self._pool.shutdown()
        self._stop_loop()

        super()._close()
