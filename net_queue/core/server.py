"""Communications server package"""

import uuid

from net_queue.core.comm import Communicator


__all__ = (
    "Server",
)


class Server[T](Communicator[T]):
    """Base server implementation"""

    def _handle_session_fin(self, peer: uuid.UUID, id: uuid.UUID) -> None:
        """Handle session finalize message"""
        super()._handle_session_fin(peer, id)
        self._session_fin(peer)
