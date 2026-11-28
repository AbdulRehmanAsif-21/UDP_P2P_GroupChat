# UDP P2P GroupChat Protocol Specification

## 1. Overview

The application uses UDP for peer-to-peer communication. A custom protocol provides message identification, reliable delivery, acknowledgements, and file chunk transfer.

## 2. Packet Format

Each packet consists of a fixed-size header followed by a variable-length payload.

| Field | Size (bytes) | Description |
|---|---:|---|
| Magic | 2 | Protocol identifier (`PG`) |
| Version | 1 | Protocol version |
| Message Type | 1 | Identifies the message |
| Flags | 1 | RELIABLE, FIN, SYN |
| Stream | 1 | Identifies a logical stream |
| Sender ID | 4 | Unique peer identifier |
| Sequence | 4 | Packet sequence number |
| Payload Length | 2 | Payload size |
| Payload | Variable | Maximum 1024 bytes |

All multi-byte header fields use network byte order (big-endian).

## 3. Message Types

| Message | Value | Purpose |
|---|---:|---|
| JOIN | 0x01 | Peer joins the network |
| JOIN_ACK | 0x02 | Join acknowledgement |
| HEARTBEAT | 0x03 | Peer liveness check |
| LEAVE | 0x04 | Peer leaves the network |
| CHAT | 0x10 | Group chat message |
| CHAT_PRIVATE | 0x11 | Private chat message |
| FILE_ANNOUNCE | 0x20 | Announce a shared file |
| FILE_LIST | 0x21 | Request or provide file listings |
| STORE_CHUNK | 0x22 | Store a file chunk |
| CHUNK_STORED | 0x23 | Confirm chunk storage |
| CHUNK_REQUEST | 0x24 | Request a file chunk |
| CHUNK_DATA | 0x25 | Transfer file chunk data |
| CHUNK_ERROR | 0x26 | Report a chunk error |
| LOCATION_UPDATE | 0x27 | Update chunk locations |
| ACK | 0xFE | Acknowledge reliable packet |

## 4. Flags

| Flag | Value | Description |
|---|---:|---|
| RELIABLE | 0x01 | Packet requires acknowledgement |
| FIN | 0x02 | Last packet of a transfer |
| SYN | 0x04 | First packet of a new sender session |

Flags can be combined using bitwise OR.

## 5. Reliability

Reliable messages use a selective-repeat sliding-window protocol.

- Sender window size: 32 packets.
- Receiver window size: 64 packets.
- Initial retransmission timeout: 300 ms.
- Minimum retransmission timeout: 100 ms.
- Maximum retransmission timeout: 1000 ms.
- Maximum retransmissions: 8.
- Sequence numbers are 32-bit unsigned integers.
- Sequence numbers start at a random value and wrap at 2^32.
- ACK packets acknowledge individual sequence numbers.
- Out-of-order packets are buffered until missing packets arrive.
- Duplicate packets are acknowledged but not delivered again.
- RTT measurements dynamically adjust the retransmission timeout.

### Session Initialization

The first reliable packet sent to a peer on a stream carries the SYN flag.

A receiver without a matching session ignores non-SYN packets without acknowledging them.

A SYN packet with a sequence number outside the current receive window starts a fresh session.

The receiver accepts sequence numbers in the range:

`[expected, expected + 64)`

## 6. File Transfer

File data is divided into chunks of at most 1024 bytes.

Each chunk is sent as a reliable packet.

The final packet of a transfer carries the FIN flag.

The receiver reassembles the packets in order and delivers the complete blob through the transport callback.

## 7. Unreachable Peers

If a reliable packet receives no acknowledgement after eight retransmissions, the peer is reported as unreachable through the `on_unreachable(addr)` callback.

## 8. Protocol Validation

The receiver validates:

- Packet magic.
- Protocol version.
- Header size.
- Payload length.
- Total packet length.

Invalid packets are rejected.
## 6. Membership Discovery

- JOIN is sent to 255.255.255.255 and 127.0.0.1 on every port of the agreed port range (default 5000-5010), or directly to --bootstrap ip:port.

- JOIN is retried every second for up to 5 seconds.

- Peer IDs must be unique.

- HEARTBEAT messages are sent periodically to known peers.

- A peer is considered dead after 4 seconds without a heartbeat.

- LEAVE messages notify other peers when a peer exits gracefully.
