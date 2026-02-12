# net-queue
Zero-copy & lock-free network communications using queues

## Example
```python
# server.py
import net_queue as nq

with nq.new(purpose=nq.Purpose.SERVER) as queue:
    message = queue.get()
    queue.put("Hello, Client!")
```

```python
# client.py
import net_queue as nq

with nq.new(purpose=nq.Purpose.CLIENT) as queue:
    queue.put("Hello, Server!")
    message = queue.get()
```

## Benchmark
| Configuration | Used |
|-|-|
| SW | Python 3.13.5 |
| OS | Debian GNU/Linux 13 (trixie) |
| CPU | 13th Gen Intel® Core™ i5-13400 × 16 |
| RAM | 64 GB |

| Test | Transfer | Operations | Executed |
|-|-|-|-|
| Sync | 17.18 GB | 8.0 K | python test/iops.py protocol purpose sync --step-size 0 --max-size 21 --reps 4_000 |
| Async | 17.18 GB | 8.0 K | python test/iops.py protocol purpose async --step-size 0 --max-size 21 --reps 4_000 |
| Mix | 8.59 GB | 8.19 K | python test/iops.py protocol purpose async --min-size 8 --step-size 2 --step-expo 0.5 --max-size 32 |

| Time | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 5.3 s | 36.2 s | 23.1 s |
| Async | 8.5 s | 27.0 s | 20.9 s |
| Mix | 9.2 s | 23.8 s | 20.2 s |

| Transfer | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 25.24 Gbps | 3.71 Gbps | 5.81 Gbps |
| Async | 15.77 Gbps | 4.98 Gbps | 6.43 Gbps |
| Mix | 14.91 Gbps | 5.78 Gbps | 6.79 Gbps |

| Operations | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 1500.00 IOPS | 220.85 IOPS | 346.37 IOPS |
| Async | 939.73 IOPS | 296.78 IOPS | 383.04 IOPS |
| Mix | 1780.00 IOPS | 689.41 IOPS | 809.01 IOPS |

| Memory | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 21.94 MB | 8377.33 MB | 30.68 MB |
| Async | 33.39 MB | 7585.78 MB | 41.46 MB |
| Mix | 8639.32 MB | 10481.62 MB | 8642.59 MB |

## Install
### Production
```bash
pip install net-queue
```

### Development
```bash
git clone https://github.com/hpca-uji/net-queue.git
cd net-queue
pip install -e .
```

## Documentation
### Constants
- `Protocol`:

  Communication protocol

  - `TCP`
  - `MQTT` (requires external broker)
  - `GRPC`

- `Purpose`:

  Communication purpose

  - `SERVER`
  - `CLIENT`

### Structures
- `CommunicatorOptions(...)`

  Communicator options

  - `id: uuid.UUID = uuid.uuid4()` (random)
  - `netloc: NetworkLocation = NetworkLocation('127.0.0.1', 51966)`
  - `connection: ConnectionOptions = ConnectionOptions()`
  - `serialization: SerializationOptions = SerializationOptions()`
  - `security: SecurityOptions | None = None`
  - `workers: int = 1`

    Maximum number of threads to use for connection handling.
    Depending on the protocol 1~3 more maybe used, however they will be idle most of the time.
    On high throughput applications or high latency networks this may need increasing.

    Selection: Number of expected participants (less +efficiency, more +performance).  
    Default: Minimum required resources.  

- `ConnectionOptions(...)`

  Connection options

  There are no definite values, depends on: use case, OS/stack/version and link specs.
  Its recommended to test your configuration (or preferably set them dynamically).
  There are *rules of thumb* but they serve as a baseline.

  - `get_merge: bool = True`

    Merge message chunks to a contiguous memory block during receiving,
    this typically improves performance when processing large messages.

    Merged chunks are up to `message_size` size,
    internally a buffer of this size is dynamically allocated.

  - `put_merge: bool = True`

    Merge message chunks to a contiguous memory block during sending,
    this typically improves performance when processing small messages.

    Merged chunks are up to `transport_size` size,
    internally a buffer of this size is preallocated.

  - `efficient_size: int = 64 * 1024 ** 1` (64 KiB)

    Minimum chunk size to consider the send efficient before attempting merging.
    If no more chunks are queued then the chunk will be sent as-is.

    Selection: Amortize abstractions costs (less +management, more +merging).  
    Default: Maximum TCP segment size.  

  - `transport_size: int = 4 * 1024 ** 2` (4 MiB)

    Maximum chunk size to send to underlying backend before splitting.

    Selection: Bandwidth-delay product of network (less +streaming/CPU, more +bursty/RAM).  
    Default: Typical connection (80Mbps @ 50ms) & balanced RAM/CPU usage.  

  - `queue_size: int = 1 ** 1024 ** 3` (1 GiM)

    Maximum queued up messages before dropping incoming messages.

    Selection: Maximum according to memory limits.  
    Default: Unlimited for typical usage.  

  - `message_size: int = 1 ** 1024 * 4` (1 TiB)

    Maximum message size to deserialize before attempting splitting.
    If the deserializer does not supports arbitrary sub-chunks,
    this setting may raise exceptions on message extraction.

    Selection: Maximum according to memory limits.  
    Default: Unlimited for typical usage.  

- `SerializationOptions(...)`

  Serialization options

  - `load: Callable[[Stream], Any] = PickleSerializer().load`

    Message deserialization handler

  - `dump: Callable[[Any], Stream] = PickleSerializer().dump`

    Message serialization handler

- `SecurityOptions(...)`

  Security options

  - `key: Path | None = None`

    Server's private key

    Required for servers, for clients always `None`.

  - `certificate: Path | None = None`

    Server's certificate chain or client's trust chain

    Required for servers, for clients if not provided, it defaults to the system's chain.

- `NetworkLocation(...)`

  Network location

  Extends: `NamedTuple`

  - `host: str = "127.0.0.1"`
  - `port: int = 51966`

### Functions
- `new(protocol, purpose, options)`

  Create a communicator.

  - `protocol: Protocol = Protocol.TCP`
  - `purpose: Purpose = Purpose.Client`
  - `options: CommunicatorOptions = CommunicatorOptions()`

### Classes
- `core.Communicator(options)`

  Communicator implementation

  Operations are thread-safe.

  Communicator has `with` support.

  - `options: CommunicatorOptions = CommunicatorOptions()`

  ---

  - `id: uuid.UUID` (helper for `options.id`)
  - `options: CommunicatorOptions`

  - `put(data: Any, *peers: uuid.UUID) -> Future[None]`

    Publish data to peers

    For clients if no peers are defined, data is send to the server.
    For servers if no peers are defined, data is send to all clients.

    It is preferred to specify multiple peers instead of issuing multiple puts,
    as data will only be serialized once and protocols may use optimized routes.

    Note: Only servers can send to a particular client.

    Future is resolved when data is safe to mutate again.
    Future may raise `ConnectionError(uuid.UUID)` if the peer or itself are closed.
    Future may raise protocol specific exceptions.

  - `get(*peers: uuid.UUID) -> Any`

    Get data from peers

    If no peers are defined, data is returned from the first available peer.

    Note: Currently peers can not be specified.

  - `close() -> None`

    Close the communicator

- `{protocol}.{purpose}.Communicator(options)`

  Concrete communicator implementation for the given protocol and purpose

- `utils.stream.Stream()`

  Zero-copy non-blocking pipe-like

  Interface mimics a non-blocking BufferedRWPair,
  but operations return memoryviews instead of bytes.

  Operations are not thread-safe.
  Reader is responsible of releasing chunks.
  Writer hands off responsibility over chunks.

  Stream has `with` and `bytes` support.
  Stream has `copy.copy()` support, however it does not support `copy.deepcopy()`.

  Extends: `BufferedIOBase`

  - `nchunks -> int`

    Number of chunks held in stream

  - `nbytes -> int`

    Number of bytes held in stream

  - `empty() -> bool`

    Is stream empty (would read block)

  - `readchunk() -> memoryview`

    Read a chunk from stream

  - `unreadchunk(chunk: memoryview) -> int`

    Unread a chunk into the stream

  - `readchunk() -> memoryview`

    Read a chunk from stream

  - `unwritechunk() -> memoryview`

    Unwrite a chunk from the stream

  - `writechunk(chunk: memoryview) -> int`

    Write a chunk into the stream

  - `peekchunk() -> memoryview`

    Peek a chunk from stream

  - `readchunks() -> Iterable[memoryview]`

    Read all chunks from stream

  - `writechunks(chunks: Iterable[memoryview]) -> int`

    Write many chunks into the stream

  - `update(bs: Iterable[Buffer]) -> int`

    Write many buffers into the stream

  - `clear() -> None`

    Release all chunks

  - `copy() -> Stream`

    Shallow copy of stream

  - `tobytes() -> bytes`

      Transform stream to bytes (will copy)

  - `frombytes(b: Buffer) -> Stream`

      Construct a stream from bytes

- `utils.streamtools.PickleSerializer(...)`

  Pickle-stream serializer

  **Warning**: The `pickle` module is not secure. Only unpickle data you trust.

  - `restrict: Iterable[str] | None = None`

    If defined it limits the range of trusted types.

    Example: `["builtins"]` for a whole module

    Example: `["uuid.UUID"]` for a single class

    *Note*: Some builtins may be implicitly allowed due to optimizations.

  ---

  - `load(data: Any) -> Stream`

    Transform a data into a stream

  - `dump(data: Stream) -> Any`

    Transform a stream into useful data

- `utils.streamtools.BytesSerializer(...)`

  Bytes-stream serializer

  - `load(data: bytes) -> Stream`

    Transform bytes into a stream

  - `dump(data: Stream) -> bytes`

    Transform a stream into bytes

## Notes
### Communication conventions
- ini: connection start (identify)
- fin: connection stop  (flush)
- com: message exchange (generic)
- c2s: message exchange (client -> server)
- s2c: message exchange (server -> client)

### Communication handshakes
Ini:
- Server & client sends ID
- Server & client wait for ID
- Server create session or continues session

Fin:
- Server & client flushes message queue
- Server & client sends ID
- Server & client wait for ID

### Communication persistency
Ini:
- Must be done on first or changing connection

Fin:
- Must be done on session end (not connection)

### Communication contract
Constructor
- Never blocks
- Only one communicator per ID
- Reusing ID retain server queues

Put
- Never blocks
- Communication will not modify object
- Consumer must not modify object util future resolved
- Resolved futures acknowledge peer reception
- ConnectionError error futures indicates peer disconnected

Get
- Always block
- Returns a message or raises ConnectionError
- Once closed it continues working until exhausted then it raises ConnectionError

Close
- Always block
- Server waits for peers to disconnect

### TCP
Library: socket  
Parallelism: Thread pool (n+1+1 threads)  

### MQTT
Library: paho-mqtt  
Options: tcp transport, 0 QOS, 3.1.1 protocol  
Parallelism: Single threaded (1+1+1 threads)  

MQTT broker implementations are not common, so the server provided here is actually another client. Therefore the address and port provided to both, the client and server, should be the one of the actual broker, not where the server is running.

The MQTT library handles communications single-threaded, therefore operations on related callbacks are limited to pushing or pulling data from queues without blocking, so all operations are minimal and fast.

Peer-groups and global communications are not optimized.

First, chunked message ordering must be resolved. Single chunk order it is guaranteed by the protocol, even on with different topics. Second, peer-groups could be implemented using grouping requests that generate new UUID per group. This would reduce also reduce load on the broker.

### gRPC
Library: grpcio  
Options: compression disabled, protobuf disabled  
Parallelism: Thread pool (n+1+? threads)  

gRPC does not conform well to a async send & async receive model, it expects remote procedure calls to be called, processed and responded. To simulate this model we created a bidirectional streaming procedure. Sent data is queued at the server, received data is polled until available.

This also means there is no eager client reception or eager server send, so polling is required.

Polling is implemented with a exponential backoff time and a limit. The gRPC library queues requests, so requests would always be replied in a timely manner, but we do not want to hogg the CPU or network with useless requests.

It is important to not hold the procedures indefinitely, since this could starve the server of threads. Additionally, if a streaming direction was already closed, messages could end up queued forever if not restarted.

To alleviate network latency queues are flushed unidirectionally in turns, instead of interleaving directions. However on high throughput applications this could lead to a very bursty receive pattern.

## Planned
Implement reconnection support. The protocol already has support for it, server support is done, clients can reconnect but can not yet disconnect without flushing.

Implement two-way connection expiration and keep-alives. There is no reliable way to track connection drops between communication implementations. Most of them end up with memory leaks. If desired expiration periods could be long and automatic client reconnections could be allowed, enabling MQTT-like reliability without the cost.

Implement message cancelling support. It is already plausible to cancel a message if it is queued but not buffered. However changing the future from a pending state to running would cause a lock acquire.

## Acknowledgments
The library has been partially supported by:
- Project PID2023-146569NB-C22 "Inteligencia sostenible en el Borde-UJI" funded by the Spanish Ministry of Science, Innovation and Universities.
- Project C121/23 Convenio "CIBERseguridad post-Cuántica para el Aprendizaje FEderado en procesadores de bajo consumo y aceleradores (CIBER-CAFE)" funded by the Spanish National Cybersecurity Institute (INCIBE).

![](footer.jpg)