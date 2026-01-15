"""Communications API test"""

import sys
import copy
import enum
from pathlib import Path
from argparse import ArgumentParser, Namespace

import net_queue as nq


__all__ = ()


MSG = "Hello, World!"


class Peer(enum.StrEnum):
    """Peer type"""
    SERVER = enum.auto()
    CLIENT = enum.auto()


# Argument pasrser
parser = ArgumentParser(prog="nq-test-api", description="net-queue API test")
parser.add_argument("proto", choices=list(nq.Protocol), help="Which protocol to use")
parser.add_argument("peer", choices=list(Peer), help="Which peer type to use")
parser.add_argument("--size", type=int, default=1, help="Number of expected clients for the server")
parser.add_argument("--secure", action="store_true", default=False, help="Enable secure communications")


def get_options(config: Namespace) -> nq.CommunicatorOptions:
    """Get communicator options"""
    options = nq.CommunicatorOptions()

    if config.secure:
        security = nq.SecurityOptions(key=Path("key.pem"), certificate=Path("cert.pem"))
        config.options = copy.replace(config.options, security=security)

    return options


def server(config: Namespace):
    """Server mode"""
    clients = set()
    server_msg = MSG
    server = nq.new(protocol=config.proto, purpose=nq.Purpose.SERVER, options=get_options(config))
    print(server)

    for _ in range(config.size):
        client_msg = server.get()
        print(f"{server.id}-c2s: {client_msg}")
        clients.add(client_msg.peer)

    print(f"{server.id}-s2c-global: {server_msg}")
    future = server.put(server_msg)
    future.result()

    for client in clients:
        print(f"{server.id}-s2c-local: {server_msg}")
        server.put(server_msg, client)

    server.close()


def client(config: Namespace):
    """Client mode"""
    client_msg = MSG
    client = nq.new(protocol=config.proto, purpose=nq.Purpose.CLIENT, options=get_options(config))
    print(client)

    print(f"{client.id}-c2s: {client_msg}")
    future = client.put(client_msg)
    future.result()

    server_msg = client.get()
    print(f"{client.id}-s2c-global: {server_msg}")
    assert client_msg == server_msg.data, "Corrupted message data"  # type: ignore

    server_msg = client.get()
    print(f"{client.id}-s2c-local: {server_msg}")
    assert client_msg == server_msg.data, "Corrupted message data"

    client.close()


def main(config: Namespace):
    """Application entrypoint"""
    self = sys.modules[__name__]
    handler = getattr(self, config.peer)
    print(config)
    handler(config)


if __name__ == "__main__":
    main(parser.parse_args())
