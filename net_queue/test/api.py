"""net-queue API test"""

import sys
from pathlib import Path
from argparse import ArgumentParser, Namespace

import net_queue as nq


__all__ = ()


MSG = "Hello, World!"


def get_options(config: Namespace) -> nq.CommunicatorOptions:
    """Get communicator options"""
    return nq.CommunicatorOptions(
        netloc=nq.NetworkLocation(
            host=config.host,
            port=config.port,
        ),
        connection=nq.ConnectionOptions(
            get_merge=config.get_merge,
            put_merge=config.put_merge,
            transport_size=config.transport_size,
        ),
        security=nq.SecurityOptions(
            key=Path("key.pem"),
            certificate=Path("cert.pem")
        ) if config.secure else None,
        workers=config.workers
    )


def server(config: Namespace):
    """Server handler"""
    clients = set()
    server_msg = MSG
    server = nq.new(protocol=config.protocol, purpose=nq.Purpose.SERVER, options=get_options(config))
    print(server)

    for _ in range(config.clients):
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
    """Client handler"""
    client_msg = MSG
    client = nq.new(protocol=config.protocol, purpose=nq.Purpose.CLIENT, options=get_options(config))
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
    handler = getattr(self, config.purpose)
    print(config)
    handler(config)


if __name__ == "__main__":
    config = nq.CommunicatorOptions()
    parser = ArgumentParser(prog="nq-test-api", description="net-queue API test")
    parser.add_argument("protocol", choices=list(nq.Protocol), help="Which backend to use")
    parser.add_argument("purpose", choices=list(nq.Purpose), help="Which peer type to use")
    parser.add_argument("--clients", type=int, default=1, help="Number of expected clients for the server")
    parser.add_argument("--host", type=str, default=config.netloc.host, help="Host address to bind or connect to")
    parser.add_argument("--port", type=int, default=config.netloc.port, help="Host port to bind or connect to")
    parser.add_argument("--get-merge", type=bool, default=config.connection.get_merge, help="Enable get stream merging")
    parser.add_argument("--put-merge", type=bool, default=config.connection.put_merge, help="Enable put stream merging")
    parser.add_argument("--transport-size", type=int, default=config.connection.transport_size, help="Maximum put message size")
    parser.add_argument("--secure", action="store_true", default=False, help="Enable secure communications")
    parser.add_argument("--workers", type=int, default=config.workers, help="Number of workers to use")
    main(parser.parse_args())
