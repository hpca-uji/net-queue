"""MQTT server package"""

import uuid
from concurrent.futures import Future

import paho.mqtt.client as mqtt_client

from net_queue.mqtt import Protocol
from net_queue.utils import asynctools
from net_queue.core.server import Server
from net_queue.utils.stream import Stream
from net_queue.core.comm import CommunicatorOptions


__all__ = (
    "Communicator",
)


# Sentinel objects
END_COMM = b""


class Communicator(Protocol, Server[str]):
    """MQTT server"""

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Server initialization"""
        super().__init__(options)

        # MQTT
        self._register_handler(topic="c2s/+", handler=self._c2s)

    def _connection_post_ini(self, peer: uuid.UUID) -> None:
        comm = self._comms.pop(peer)
        state = self._sessions.pop(peer)
        peer = uuid.UUID(hex=comm)
        self._comms[peer] = comm
        self._sessions[peer] = state

    def _c2s(self, client: mqtt_client.Client, userdata, mqtt_message: mqtt_client.MQTTMessage) -> None:
        """Client message handler"""
        # NOTE: communication thead
        comm = self._peer(mqtt_message)
        peer = self._set_default_peer(comm)
        session = self._sessions[peer]

        data = mqtt_message.payload
        session.get_write(data)
        self._process_gets(peer)
        peer = session.peer

    def _put(self, stream: Stream, peer: uuid.UUID) -> Future[None]:
        """Put stream into queue and notify"""
        future = super()._put(stream, peer)
        self._pool.submit(self._s2c, peer).add_done_callback(asynctools.future_warn_exception)
        return future

    def _s2c(self, peer: uuid.UUID):
        """Server to client communication"""
        comm = self._comms[peer]
        session = self._sessions[peer]

        size = 0
        session.put_flush_queue()
        for view in session.put_flush_buffer():
            with view:
                self._publish(f"s2c/{comm}", bytes(view))
                size += len(view)
        self._put_commit(peer, size)

        if not session.state and session.put_empty():
            self._connection_fin(comm)

    def _close(self) -> None:
        """Close the server"""

        # Wait peers to drain
        with self._lock:
            while self._comms:
                self._lock.wait()

        # Close resources
        self._pool.shutdown()
        self._stop_loop()

        super()._close()
