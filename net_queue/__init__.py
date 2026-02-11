"""Communications package"""

import enum
import importlib

from net_queue.core.comm import Communicator
from net_queue.core import CommunicatorOptions, NetworkLocation, ConnectionOptions, SerializationOptions, SecurityOptions


__all__ = (
    "Protocol",
    "Purpose",
    "CommunicatorOptions",
    "NetworkLocation",
    "ConnectionOptions",
    "CommunicatorOptions",
    "SerializationOptions",
    "SecurityOptions",
    "new"
)


class Protocol(enum.StrEnum):
    """Communication protocol"""
    TCP = enum.auto()
    MQTT = enum.auto()
    GRPC = enum.auto()


class Purpose(enum.StrEnum):
    """Communication purpose"""
    SERVER = enum.auto()
    CLIENT = enum.auto()


def new(protocol: Protocol = Protocol.TCP, purpose: Purpose = Purpose.CLIENT, options: CommunicatorOptions = CommunicatorOptions()) -> Communicator:
    """Generate communicator"""
    module = importlib.import_module(f"{__name__}.{protocol}.{purpose}")
    cls: type[Communicator] = getattr(module, "Communicator")
    return cls(options)
