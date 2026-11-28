# Module Interfaces

## Contract Between Abdul Rehman and Tayyab

This document defines the interfaces between the project's modules. Both developers must follow these function names, parameters, and callback formats to ensure compatibility.

## 1. reliable.py (Abdul Rehman)

```python
class ReliableTransport:
    def __init__(self, my_id, port, on_message, on_blob, on_unreachable, loss_rate=0.0)

    def start(self)
    def stop(self)

    def send(self, addr, msg_type, payload=b"", stream=0, reliable=True)

    def send_blob(self, addr, msg_type, data, stream)

    def alloc_stream(self) -> int

    stats: dict
```

Callbacks:

- `on_message(msg, addr)` — called for each in-order small message.
- `on_blob(msg_type, sender_id, stream, data, addr)` — called when a blob is fully received.
- `on_unreachable(addr)` — called after 8 failed retries.

Blob packets must be split into 1024-byte chunks, with FIN on the last packet.

Statistics must include sent packets, retransmissions, ACKs, and bytes.

## 2. membership.py (Tayyab)

```python
class Membership:
    def __init__(self, my_id, port, name, transport, on_peer_joined, on_peer_dead)
    
    def name_of(self, peer_id) -> str
    def peer_unreachable(self, addr)
    def stop(self)
    
    def join(self, bootstrap=None)
    def handle(self, msg, addr)
    def leave(self)

    def live_ids(self) -> list[int]
    def addr_of(self, peer_id) -> (ip, port)
```

Responsibilities:

- JOIN
- JOIN_ACK
- HEARTBEAT
- LEAVE
- Dead-peer detection

## 3. storage.py (Abdul Rehman)

```python
def make_manifest(path) -> dict

def holders_for(file_id, chunk, live_ids, r=2) -> list

class ChunkStore:
    def put(self, file_id, chunk, data)
    def get(self, file_id, chunk) -> bytes | None
    def has(self, file_id, chunk) -> bool
    def delete(self, file_id, chunk)

class FileNode:
    def __init__(self, my_id, transport, membership, store)

    def share(self, path)

    def handle(self, msg, addr)

    manifests: dict
    holders: dict
```

Manifest format:

- file_id
- name
- size
- chunk_size
- sha
- chunks containing index and SHA hash

Use rendezvous hashing to determine the top replica holders.

FileNode must handle:

- FILE_ANNOUNCE
- FILE_LIST
- STORE_CHUNK
- CHUNK_STORED
- CHUNK_REQUEST
- CHUNK_ERROR
- LOCATION_UPDATE

## 4. transfer.py (Tayyab)

```python
class Downloader:
    def __init__(self, my_id, transport, membership, filenode)

    def download(self, file_id, out_path) -> bool

    def on_blob(self, msg_type, sender_id, stream, data, addr)

    def on_peer_dead(self, peer_id)
```

Responsibilities:

- Multi-source parallel downloads.
- Chunk hash verification.
- Failover.
- Re-requesting chunks from alternative replicas.

## 5. chat.py (Tayyab)

```python
class Chat:
    def __init__(self, my_id, transport, membership)

    def send_group(self, text)
    def send_private(self, to_id, text)

    def handle(self, msg, addr)
```

Responsibilities:

- Group messaging.
- Private messaging.
- Display messages in `[name] text` format.
