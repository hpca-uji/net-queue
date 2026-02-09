"""Communications package"""

import uuid
import enum
import typing
import dataclasses
from pathlib import Path
from typing import NamedTuple
from dataclasses import dataclass
from collections import abc as col_abc
from net_queue.utils.stream import Stream
from net_queue.utils.streamtools import PickleSerializer


__all__ = (
    "Message",
    "SessionState",
    "NetworkLocation",
    "ConnectionOptions",
    "SerializationOptions",
    "SecurityOptions",
    "CommunicatorOptions",
)


@dataclass(slots=True, frozen=True)
class Message[T]:
    """Message object"""
    peer: uuid.UUID
    data: T


class SessionState(enum.Flag):
    """Session state"""
    READABLE = enum.auto()
    WRITABLE = enum.auto()


class NetworkLocation(NamedTuple):
    """Network location"""
    host: str = "127.0.0.1"
    port: int = 51966

    def __str__(self):
        """Unified network location"""
        return f"{self.host}:{self.port}"


@dataclass(order=False, slots=True, frozen=True)
class ConnectionOptions:
    """
    Connection options

    There are no definite values, depends on: use case, OS/stack/version and link specs.
    Its recommended to test your configuration (or preferably set them dynamically).
    There are *rules of thumb* but they serve as a baseline.
    """

    get_merge: bool = True
    put_merge: bool = True
    efficient_size: int = 64 * 1024 ** 1
    transport_size: int = 4 * 1024 ** 2
    queue_size: int = 1 * 1024 ** 3
    message_size: int = 1 * 1024 ** 4


@dataclass(order=False, slots=True, frozen=True)
class SerializationOptions:
    """Serialization options"""
    load: col_abc.Callable[[Stream], typing.Any] = PickleSerializer().load
    dump: col_abc.Callable[[typing.Any], Stream] = PickleSerializer().dump


@dataclass(order=False, slots=True, frozen=True)
class SecurityOptions:
    """Security options"""
    key: Path | None = None
    certificate: Path | None = None


@dataclass(order=False, slots=True, frozen=True)
class CommunicatorOptions:
    """Communicator options"""
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    netloc: NetworkLocation = NetworkLocation()
    connection: ConnectionOptions = ConnectionOptions()
    serialization: SerializationOptions = SerializationOptions()
    security: SecurityOptions | None = None
    workers: int = 1
