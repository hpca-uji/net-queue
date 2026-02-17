"""gRPC package"""

import sys
import uuid
from collections import abc

_path = sys.path.pop()
try:
    # global package
    import grpc
finally:
    sys.path.append(_path)

from net_queue.core.comm import Communicator  # noqa: E402
from net_queue.core import CommunicatorOptions  # noqa: E402


__all__ = (
    "Protocol",
)


class Protocol[T](Communicator[T]):
    """Shared base gRPC implementation"""
    _compression = grpc.Compression.NoCompression

    def __init__(self, options: CommunicatorOptions = CommunicatorOptions()) -> None:
        """Initialize protocol"""
        super().__init__(options)
        self._grpc_options = {"grpc.max_receive_message_length": self.options.connection.transport_size, "grpc.max_send_message_length": self.options.connection.transport_size}

    def _put_flush(self, peer: uuid.UUID) -> abc.Generator[abc.Buffer]:
        """Transforms state to message"""
        session = self._sessions[peer]

        size = 0
        session.put_flush_queue()
        for view in session.put_flush_buffer():
            with view:
                yield bytes(view)
                size += len(view)
        self._put_commit(peer, size)
