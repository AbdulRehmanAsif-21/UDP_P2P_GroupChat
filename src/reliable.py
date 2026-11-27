"""
Reliable UDP transport: selective repeat, sliding window, adaptive RTO.
"""

import math
import random
import socket
import threading
import time
import traceback

from collections import OrderedDict, deque

import protocol as P


# ============================================================
# CONSTANTS
# ============================================================

WINDOW = 32
RECV_WINDOW = 64
OLD_WINDOW = 1024

MAX_RETRIES = 8

RTO_INIT = 0.3
RTO_MIN = 0.1
RTO_MAX = 1.0

TICK = 0.02

MASK = 0xFFFFFFFF

BLOB_TYPES = (P.CHUNK_DATA,)


# ============================================================
# PACKET STRUCTURES
# ============================================================

class _Pkt:
    def __init__(self, msg_type, payload, flags, seq):
        self.msg_type = msg_type
        self.payload = payload
        self.flags = flags
        self.seq = seq

        self.sent_at = 0.0
        self.retries = 0


class _SendState:
    def __init__(self):
        self.next_seq = random.getrandbits(32)
        self.first = True

        self.pending = deque()
        self.inflight = OrderedDict()


class _RecvState:
    def __init__(self):
        self.expected = None
        self.buffer = {}

        self.old = deque(maxlen=OLD_WINDOW)

        self.session_started = False
        self.session_start_seq = None


class _Rtt:
    """
    RFC 6298-style RTT estimator.
    """

    def __init__(self):
        self.srtt = None
        self.rttvar = None
        self.rto = RTO_INIT

    def update(self, sample):
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2
        else:
            self.rttvar = (
                0.75 * self.rttvar
                + 0.25 * abs(self.srtt - sample)
            )

            self.srtt = (
                0.875 * self.srtt
                + 0.125 * sample
            )

        self.rto = self.srtt + max(0.01, 4 * self.rttvar)

        self.rto = max(
            RTO_MIN,
            min(RTO_MAX, self.rto)
        )


# ============================================================
# RELIABLE TRANSPORT
# ============================================================

class ReliableTransport:

    def __init__(
        self,
        my_id,
        port,
        on_message,
        on_blob,
        on_unreachable,
        loss_rate=0.0
    ):

        self.sender_id = my_id

        self.on_message = on_message
        self.on_blob = on_blob
        self.on_unreachable = on_unreachable

        self.loss_rate = loss_rate

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        self.sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            1
        )

        self.sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_RCVBUF,
            1024 * 1024
        )

        self.sock.bind(("", port))
        self.sock.settimeout(0.2)

        self.lock = threading.RLock()

        self.running = False

        self.rx_thread = None
        self.tx_thread = None

        self.send_states = {}
        self.recv_states = {}

        self.rtt_states = {}

        self.blob_buffers = {}

        self.next_stream = random.randint(2, 255)

        self.stats = {
            "sent": 0,
            "retransmits": 0,
            "acks_sent": 0,
            "acks_received": 0,
            "duplicates": 0,
            "bytes_sent": 0,
            "sim_dropped": 0,
            "unreachable": 0,
            "delivered": 0,
        }

    # ========================================================
    # PUBLIC API
    # ========================================================

    def start(self):

        if self.running:
            return

        self.running = True

        self.rx_thread = threading.Thread(
            target=self._receive_loop,
            daemon=True
        )

        self.tx_thread = threading.Thread(
            target=self._timer_loop,
            daemon=True
        )

        self.rx_thread.start()
        self.tx_thread.start()

    def stop(self):

        if not self.running:
            return

        self.running = False

        if self.rx_thread:
            self.rx_thread.join(timeout=1)

        if self.tx_thread:
            self.tx_thread.join(timeout=1)

        try:
            self.sock.close()
        except OSError:
            pass

    def alloc_stream(self):

        with self.lock:

            stream = self.next_stream

            self.next_stream += 1

            if self.next_stream > 255:
                self.next_stream = 2

            return stream

    def send(
        self,
        addr,
        msg_type,
        payload=b"",
        stream=0,
        reliable=True,
        fin=False
    ):

        if isinstance(payload, str):
            payload = payload.encode()

        flags = 0

        if reliable:
            flags |= P.FLAG_RELIABLE

        if fin:
            flags |= P.FLAG_FIN

        with self.lock:

            if not reliable:

                packet = P.pack_message(
                    msg_type,
                    flags,
                    stream,
                    0,
                    self.sender_id,
                    payload
                )

                self._raw_send(packet, addr)
                return

            key = (addr, stream)

            state = self.send_states.get(key)

            if state is None:
                state = _SendState()
                self.send_states[key] = state

            if state.first:
                flags |= P.FLAG_SYN
                state.first = False

            seq = state.next_seq

            state.next_seq = (
                state.next_seq + 1
            ) & MASK

            packet = _Pkt(
                msg_type,
                payload,
                flags,
                seq
            )

            state.pending.append(packet)

    def send_blob(
        self,
        addr,
        msg_type,
        data,
        stream
    ):

        if isinstance(data, str):
            data = data.encode()

        max_size = P.MAX_PAYLOAD

        total = len(data)

        offset = 0

        while offset < total:

            chunk = data[offset:offset + max_size]

            fin = (
                offset + len(chunk) >= total
            )

            self.send(
                addr,
                msg_type,
                chunk,
                stream=stream,
                reliable=True,
                fin=fin
            )

            offset += len(chunk)

    def forget_peer(self, addr):

        with self.lock:

            keys = [
                key for key in self.send_states
                if key[0] == addr
            ]

            for key in keys:
                del self.send_states[key]

            keys = [
                key for key in self.recv_states
                if key[0] == addr
            ]

            for key in keys:
                del self.recv_states[key]

            keys = [
                key for key in self.rtt_states
                if key[0] == addr
            ]

            for key in keys:
                del self.rtt_states[key]

    # ========================================================
    # RAW UDP SENDING
    # ========================================================

    def _raw_send(self, packet, addr):

        if self.loss_rate > 0:

            if random.random() < self.loss_rate:

                self.stats["sim_dropped"] += 1
                return

        try:

            self.sock.sendto(packet, addr)

            self.stats["sent"] += 1
            self.stats["bytes_sent"] += len(packet)

        except OSError:

            self.stats["unreachable"] += 1

    # ========================================================
    # RECEIVE LOOP
    # ========================================================

    def _receive_loop(self):

        while self.running:

            try:

                data, addr = self.sock.recvfrom(65535)

            except socket.timeout:
                continue

            except OSError:
                break

            try:

                msg = P.unpack_message(data)

                self._handle_message(msg, addr)

            except Exception:

                traceback.print_exc()

    # ========================================================
    # MESSAGE HANDLING
    # ========================================================

    def _handle_message(self, msg, addr):

        msg_type = msg.msg_type
        flags = msg.flags
        stream = msg.stream
        seq = msg.seq
        sender_id = msg.sender_id
        payload = msg.payload

        if msg_type == P.ACK:

            self._handle_ack(
                addr,
                stream,
                payload
            )

            return

        reliable = bool(
            flags & P.FLAG_RELIABLE
        )

        if not reliable:

            self._safe(
                self.on_message,
                msg,
                addr
            )

            return

        self._handle_reliable(
            msg,
            addr,
            sender_id
        )

    # ========================================================
    # RELIABLE RECEIVE
    # ========================================================

    def _handle_reliable(
        self,
        msg,
        addr,
        sender_id
    ):

        stream = msg.stream
        seq = msg.seq
        flags = msg.flags

        key = (addr, stream)

        with self.lock:

            state = self.recv_states.get(key)

            is_syn = bool(
                flags & P.FLAG_SYN
            )

            if state is None:

                if not is_syn:
                    return

                state = _RecvState()

                state.expected = seq
                state.session_started = True
                state.session_start_seq = seq

                self.recv_states[key] = state

            elif not state.session_started:

                if not is_syn:
                    return

                state.expected = seq
                state.session_started = True
                state.session_start_seq = seq

            elif is_syn and seq != state.session_start_seq:

                distance = (
                    seq - state.expected
                ) & MASK

                if distance >= RECV_WINDOW:

                    state = _RecvState()

                    state.expected = seq
                    state.session_started = True
                    state.session_start_seq = seq

                    self.recv_states[key] = state

            distance = (
                seq - state.expected
            ) & MASK

            if distance >= RECV_WINDOW:

                self.stats["duplicates"] += 1

                self._send_ack(
                    addr,
                    stream,
                    seq
                )

                return

            if seq in state.buffer:

                self.stats["duplicates"] += 1

                self._send_ack(
                    addr,
                    stream,
                    seq
                )

                return

            state.buffer[seq] = msg

            self._send_ack(
                addr,
                stream,
                seq
            )

            delivered = []

            while state.expected in state.buffer:

                current = state.buffer.pop(
                    state.expected
                )

                delivered.append(current)

                state.old.append(
                    state.expected
                )

                state.expected = (
                    state.expected + 1
                ) & MASK

        for current in delivered:

            self._deliver(
                current,
                addr,
                sender_id
            )

    # ========================================================
    # ACK HANDLING
    # ========================================================

    def _send_ack(
        self,
        addr,
        stream,
        seq
    ):

        payload = P.pack_ack_payload(seq)

        packet = P.pack_message(
            P.ACK,
            0,
            stream,
            seq,
            self.sender_id,
            payload
        )

        self._raw_send(packet, addr)

        self.stats["acks_sent"] += 1

    def _handle_ack(
        self,
        addr,
        stream,
        payload
    ):

        try:

            ack_seq = P.unpack_ack_payload(
                payload
            )

        except Exception:
            return

        key = (addr, stream)

        with self.lock:

            state = self.send_states.get(key)

            if state is None:
                return

            packet = state.inflight.pop(
                ack_seq,
                None
            )

            if packet is None:
                return

            self.stats["acks_received"] += 1

            if packet.retries == 0:

                sample = (
                    time.monotonic()
- packet.sent_at
                )

                rtt = self.rtt_states.get(key)

                if rtt is None:

                    rtt = _Rtt()
                    self.rtt_states[key] = rtt

                rtt.update(sample)

    # ========================================================
    # TIMER AND RETRANSMISSION LOOP
    # ========================================================

    def _timer_loop(self):

        while self.running:

            now = time.monotonic()

            dead = []

            with self.lock:

                for key, state in list(
                    self.send_states.items()
                ):

                    addr, stream = key

                    rtt = self.rtt_states.get(key)

                    rto = (
                        rtt.rto
                        if rtt
                        else RTO_INIT
                    )

                    # Send new packets while window allows.

                    while state.pending and len(state.inflight) < WINDOW:
                        next_packet = state.pending[0]
                        if state.inflight:
                            oldest_seq = next(iter(state.inflight))
                            distance = (
                                next_packet.seq - oldest_seq
                            ) & MASK
                            if distance >= WINDOW:
                                break
                        packet = state.pending.popleft()

                        raw = P.pack_message(
                            packet.msg_type,
                            packet.flags,
                            stream,
                            packet.seq,
                            self.sender_id,
                            packet.payload
                        )

                        packet.sent_at = now

                        state.inflight[
                            packet.seq
                        ] = packet

                        self._raw_send(raw, addr)

                    # Check packets waiting for ACK.

                    for seq, packet in list(
                        state.inflight.items()
                    ):

                        if now - packet.sent_at < rto:
                            continue

                        if packet.retries >= MAX_RETRIES:

                            dead.append(addr)

                            del state.inflight[seq]

                            continue

                        packet.retries += 1
                        packet.sent_at = now

                        self.stats["retransmits"] += 1

                        raw = P.pack_message(
                            packet.msg_type,
                            packet.flags,
                            stream,
                            packet.seq,
                            self.sender_id,
                            packet.payload
                        )

                        self._raw_send(raw, addr)

            for addr in set(dead):

                self._notify_unreachable(addr)

            time.sleep(TICK)

    # ========================================================
    # MESSAGE DELIVERY
    # ========================================================

    def _deliver(
        self,
        msg,
        addr,
        sender_id
    ):

        self.stats["delivered"] += 1

        if msg.msg_type in BLOB_TYPES:

            self._deliver_blob(
                msg,
                addr,
                sender_id
            )

        else:

            self._safe(
                self.on_message,
                msg,
                addr
            )

    # ========================================================
    # BLOB REASSEMBLY
    # ========================================================

    def _deliver_blob(
        self,
        msg,
        addr,
        sender_id
    ):

        key = (
            addr,
            msg.stream
        )

        with self.lock:

            buffer = self.blob_buffers.get(key)

            if buffer is None:

                buffer = bytearray()

                self.blob_buffers[key] = buffer

            buffer.extend(msg.payload)

            finished = bool(
                msg.flags & P.FLAG_FIN
            )

            if not finished:
                return

            data = bytes(buffer)

            del self.blob_buffers[key]

        self._safe(
            self.on_blob,
            msg.msg_type,
            sender_id,
            msg.stream,
            data,
            addr
        )

    # ========================================================
    # UNREACHABLE PEER HANDLING
    # ========================================================

    def _notify_unreachable(self, addr):

        self.stats["unreachable"] += 1

        self._safe(
            self.on_unreachable,
            addr
        )

    # ========================================================
    # CALLBACK SAFETY
    # ========================================================

    @staticmethod
    def _safe(fn, *args):

        try:

            if fn:
                fn(*args)

        except Exception:

            traceback.print_exc()