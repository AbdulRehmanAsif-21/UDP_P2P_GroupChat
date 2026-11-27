"""
UDP P2P GroupChat protocol.

Packet format:
    Magic       2 bytes
    Version     1 byte
    Message     1 byte
    Flags       1 byte
    Stream      1 byte
    Sender ID   4 bytes
    Sequence    4 bytes
    Payload Len 2 bytes
    Payload     Variable (maximum 1024 bytes)
"""

import struct
from dataclasses import dataclass


# ============================================================
# PROTOCOL CONFIGURATION
# ============================================================

MAGIC = b"PG"
VERSION = 1

MAX_PAYLOAD = 1024

HEADER_FORMAT = "!2sBBBBIIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


# ============================================================
# FLAGS
# ============================================================

FLAG_RELIABLE = 0x01
FLAG_FIN = 0x02
FLAG_SYN = 0x04


# ============================================================
# MESSAGE TYPES
# ============================================================

JOIN = 0x01
JOIN_ACK = 0x02
HEARTBEAT = 0x03
LEAVE = 0x04

CHAT = 0x10
CHAT_PRIVATE = 0x11

FILE_ANNOUNCE = 0x20
FILE_LIST = 0x21
STORE_CHUNK = 0x22
CHUNK_STORED = 0x23
CHUNK_REQUEST = 0x24
CHUNK_DATA = 0x25
CHUNK_ERROR = 0x26
LOCATION_UPDATE = 0x27

ACK = 0xFE


# ============================================================
# MESSAGE STRUCTURE
# ============================================================

@dataclass
class Message:
    msg_type: int
    flags: int
    stream: int
    seq: int
    sender_id: int
    payload: bytes


# ============================================================
# PACK MESSAGE
# ============================================================

def pack_message(
    msg_type,
    flags,
    stream,
    seq,
    sender_id,
    payload=b""
):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")

    payload = bytes(payload)

    if len(payload) > MAX_PAYLOAD:
        raise ValueError(
            f"Payload exceeds maximum size of {MAX_PAYLOAD} bytes"
        )

    if not 0 <= msg_type <= 255:
        raise ValueError("Invalid message type")

    if not 0 <= flags <= 255:
        raise ValueError("Invalid flags")

    if not 0 <= stream <= 255:
        raise ValueError("Invalid stream")

    if not 0 <= seq <= 0xFFFFFFFF:
        raise ValueError("Invalid sequence number")

    if not 0 <= sender_id <= 0xFFFFFFFF:
        raise ValueError("Invalid sender ID")

    header = struct.pack(
        HEADER_FORMAT,
        MAGIC,
        VERSION,
        msg_type,
        flags,
        stream,
        sender_id,
        seq,
        len(payload)
    )

    return header + payload


# ============================================================
# UNPACK MESSAGE
# ============================================================

def unpack_message(data):

    if len(data) < HEADER_SIZE:
        raise ValueError("Packet is smaller than the header")

    (
        magic,
        version,
        msg_type,
        flags,
        stream,
        sender_id,
        seq,
        payload_len
    ) = struct.unpack(
        HEADER_FORMAT,
        data[:HEADER_SIZE]
    )

    if magic != MAGIC:
        raise ValueError("Invalid packet magic")

    if version != VERSION:
        raise ValueError("Unsupported protocol version")

    if payload_len > MAX_PAYLOAD:
        raise ValueError("Payload exceeds maximum size")

    if len(data) != HEADER_SIZE + payload_len:
        raise ValueError("Invalid packet length")

    payload = data[HEADER_SIZE:]

    return Message(
        msg_type=msg_type,
        flags=flags,
        stream=stream,
        seq=seq,
        sender_id=sender_id,
        payload=payload
    )


# ============================================================
# ACK PAYLOAD
# ============================================================

def pack_ack_payload(seq):

    if not 0 <= seq <= 0xFFFFFFFF:
        raise ValueError("Invalid ACK sequence number")

    return struct.pack("!I", seq)


def unpack_ack_payload(payload):

    if len(payload) != 4:
        raise ValueError("Invalid ACK payload")

    return struct.unpack("!I", payload)[0]