"""Communications package"""

import enum
import importlib

from net_queue.core.comm import Communicator
from net_queue.core import CommunicatorOptions, NetworkLocation, ConnectionOptions, SerializationOptions, SecurityOptions


__all__ = (
    "Backend",
    "Purpose",
    "CommunicatorOptions",
    "NetworkLocation",
    "ConnectionOptions",
    "CommunicatorOptions",
    "SerializationOptions",
    "SecurityOptions",
    "new"
)


class Backend(enum.StrEnum):
    """Communication backend"""
    SOCKET_TCP = enum.auto()
    PHAO_MQTT = enum.auto()
    GRPCIO = enum.auto()


class Purpose(enum.StrEnum):
    """Communication purpose"""
    SERVER = enum.auto()
    CLIENT = enum.auto()


def new(backend: Backend = Backend.SOCKET_TCP, purpose: Purpose = Purpose.CLIENT, options: CommunicatorOptions = CommunicatorOptions()) -> Communicator:
    """Generate communicator"""
    module = importlib.import_module(f"{__name__}.{backend}.{purpose}")
    cls: type[Communicator] = getattr(module, "Communicator")
    return cls(options)
