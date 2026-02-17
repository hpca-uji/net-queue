"""net-queue IOPS test"""

import sys
import math
import time
import enum
import random
from pathlib import Path
from threading import Thread
from argparse import ArgumentParser, Namespace

import numpy

import net_queue as nq


__all__ = ()


class Mode(enum.StrEnum):
    """Test mode"""
    SYNC = enum.auto()
    ASYNC = enum.auto()


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


def get(comm: nq.Communicator, msgs: list[bytearray]):
    """Communication get handler"""
    for i in range(len(msgs)):
        print(i, end="\r", flush=True)
        comm.get().data
    print(i)


def put(comm: nq.Communicator, msgs: list[bytearray]):
    """Communication put handler"""
    for i in range(len(msgs)):
        print(i, end="\r", flush=True)
        comm.put(msgs[i])
    print(i)


def convert_size(units: float, scale: int = 1000):
    """Convert unit to use SI suffixes"""
    size_name = ("", "K", "M", "G", "T", "P", "E", "Z", "Y")
    if units > 0:
        i = int(math.log(units, scale))
        p = math.pow(scale, i)
        s = round(units / p, 2)
    else:
        i = 0
        s = 0
    return f"{s}{size_name[i]}"


def print_stats(sizes: list[int], time: float) -> None:
    """Print statistics"""
    ops = len(sizes) * 2
    size = sum(sizes) * 2
    avg = sum(sizes) / len(sizes)
    print(f"Time:       {time:.1f}s")
    print(f"Data:       {convert_size(len(sizes))} @ {convert_size(avg)}B")
    print(f"Transfer:   {convert_size(size)}B @ {convert_size(size * 8 / time):>5}bps")  # type: ignore
    print(f"Operations: {convert_size(ops)} @ {convert_size(ops / time)}IOPS")  # type: ignore


def generate(config: Namespace) -> list[numpy.ndarray]:
    """Generate messages"""
    messages = []
    buffer = numpy.arange(2 ** config.max_size, dtype=numpy.uint8)
    if config.step_size:
        for i in range(config.min_size, config.max_size, config.step_size):
            splits = round(((2 ** config.max_size) / (2 ** i)) ** config.step_expo)
            for j in range(splits):
                messages.append(buffer[:2 ** i])
    messages.append(buffer)
    messages *= config.reps
    random.shuffle(messages)
    return messages


def server(config: Namespace):
    """Server handler"""
    messages = generate(config)
    sizes = list(map(len, messages)) * config.clients

    with nq.new(protocol=config.protocol, purpose=nq.Purpose.SERVER, options=get_options(config)) as server:
        get_thread = Thread(target=get, args=(server, messages * config.clients))
        put_thread = Thread(target=put, args=(server, messages))
        print(server)

        for _ in range(config.clients):
            server.get()
        server.put(None)
        start_time = time.time()

        match config.mode:
            case Mode.SYNC:
                time.sleep(config.delay)
                get_thread.run()
                put_thread.run()

            case Mode.ASYNC:
                get_thread.start()
                put_thread.start()
                get_thread.join()
                put_thread.join()

    end_time = time.time()

    print_stats(sizes=sizes, time=end_time - start_time)


def client(config: Namespace):
    """Client handler"""
    messages = generate(config)
    sizes = list(map(len, messages)) * config.clients

    with nq.new(protocol=config.protocol, purpose=nq.Purpose.CLIENT, options=get_options(config)) as client:
        get_thread = Thread(target=get, args=(client, messages))
        put_thread = Thread(target=put, args=(client, messages))
        print(client)

        client.put(None)
        client.get()
        start_time = time.perf_counter()

        match config.mode:
            case Mode.SYNC:
                put_thread.run()
                time.sleep(config.delay)
                get_thread.run()

            case Mode.ASYNC:
                get_thread.start()
                put_thread.start()
                get_thread.join()
                put_thread.join()

    end_time = time.perf_counter()

    print_stats(sizes=sizes, time=end_time - start_time)


def main(config: Namespace):
    """Application entrypoint"""
    self = sys.modules[__name__]
    handler = getattr(self, config.purpose)
    print(config)
    random.seed(0)
    handler(config)


if __name__ == "__main__":
    config = nq.CommunicatorOptions()
    parser = ArgumentParser(prog="nq-test-iops", description="net-queue IOPS test")
    parser.add_argument("protocol", choices=list(nq.Protocol), help="Which backend to use")
    parser.add_argument("purpose", choices=list(nq.Purpose), help="Which peer type to use")
    parser.add_argument("mode", choices=list(Mode), help="Which operation mode to use")
    parser.add_argument("--delay", type=float, default=0.0, help="Time to wait before reception start (to cause buffering)")
    parser.add_argument("--min-size", type=int, default=8, help="Exponent of minimum message size")
    parser.add_argument("--step-size", type=int, default=2, help="Exponent between message sizes")
    parser.add_argument("--max-size", type=int, default=32, help="Exponent of maximin message size")
    parser.add_argument("--step-expo", type=float, default=0.5, help="Exponent of number of splits when stepping down a size")
    parser.add_argument("--reps", type=int, default=1, help="Number of repetitions of messages")
    parser.add_argument("--clients", type=int, default=1, help="Number of expected clients for the server")
    parser.add_argument("--host", type=str, default=config.netloc.host, help="Host address to bind or connect to")
    parser.add_argument("--port", type=int, default=config.netloc.port, help="Host port to bind or connect to")
    parser.add_argument("--get-merge", type=bool, default=config.connection.get_merge, help="Enable get stream merging")
    parser.add_argument("--put-merge", type=bool, default=config.connection.put_merge, help="Enable put stream merging")
    parser.add_argument("--transport-size", type=int, default=config.connection.transport_size, help="Maximum put message size")
    parser.add_argument("--secure", action="store_true", default=False, help="Enable secure communications")
    parser.add_argument("--workers", type=int, default=config.workers, help="Number of workers to use")
    main(parser.parse_args())
