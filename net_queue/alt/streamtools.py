"""Alternative stream utilities"""

from net_queue.utils.stream import Stream
from net_queue.utils.streamtools import PickleSerializer


__all__ = (
    "FullSerializer",
)


class FullSerializer(PickleSerializer):
    """Serializer that emulates a pickle behavior"""

    def dump(self, data) -> Stream:
        """Transform a data into a stream"""
        from io import BytesIO
        stream = BytesIO()
        self._dump(obj=data, file=stream, protocol=5)
        stream = Stream.frombytes(stream.getvalue())
        return stream
