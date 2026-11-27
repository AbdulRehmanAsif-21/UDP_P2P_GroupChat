import os
import sys
import time
import struct
import random
import hashlib

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src")
)

import protocol as P
from reliable import ReliableTransport


def make(pid, port, loss=0.0):

    box = {
        "msgs": [],
        "blobs": [],
        "dead": []
    }

    def on_msg(msg, addr):
        if len(msg.payload) >= 4:
            value = struct.unpack("!I", msg.payload[:4])[0]
            box["msgs"].append(value)

    def on_blob(*args):
        box["blobs"].append(args)

    def on_dead(addr):
        box["dead"].append(addr)

    transport = ReliableTransport(
        pid,
        port,
        on_msg,
        on_blob,
        on_dead,
        loss_rate=loss
    )

    transport.start()

    return transport, box


def wait_until(condition, timeout):

    start = time.monotonic()

    while time.monotonic() - start < timeout:

        if condition():
            return True

        time.sleep(0.05)

    return False


def test_ordered_delivery_under_loss():

    print("\nTest 1: Ordered delivery under 20% packet loss")

    a, _ = make(1, 6101, loss=0.2)
    b, box = make(2, 6102, loss=0.2)

    try:

        for i in range(200):

            a.send(
                ("127.0.0.1", 6102),
                P.CHAT,
                struct.pack("!I", i),
                stream=1,
                reliable=True
            )

        success = wait_until(
            lambda: len(box["msgs"]) == 200,
            60
        )

        assert success, (
            f"Received only {len(box['msgs'])}/200 messages"
        )

        assert box["msgs"] == list(range(200)), (
            "Messages were not delivered in order"
        )

        print(
            "Ordered delivery under 20% loss OK;",
            "retransmits:",
            a.stats["retransmits"]
        )

    finally:
        a.stop()
        b.stop()


def test_blob_under_loss():

    print("\nTest 2: 64 KB blob transfer under packet loss")

    a, _ = make(1, 6103, loss=0.2)
    b, box = make(2, 6104, loss=0.2)

    try:

        data = os.urandom(65536)

        a.send_blob(
            ("127.0.0.1", 6104),
            P.CHUNK_DATA,
            data,
            stream=5
        )

        success = wait_until(
            lambda: len(box["blobs"]) == 1,
            60
        )

        assert success, "Blob transfer timed out"

        msg_type, sender_id, stream, received, addr = box["blobs"][0]

        assert msg_type == P.CHUNK_DATA
        assert sender_id == 1
        assert stream == 5

        assert hashlib.sha256(received).digest() == (
            hashlib.sha256(data).digest()
        ), "Blob contents do not match"

        print("64 KB blob under loss OK")

    finally:
        a.stop()
        b.stop()


def test_restart_resync():

    print("\nTest 3: Sender restart and session resynchronization")

    a, _ = make(1, 6105)
    b, box = make(2, 6106)

    try:

        for i in range(5):

            a.send(
                ("127.0.0.1", 6106),
                P.CHAT,
                struct.pack("!I", i),
                stream=1,
                reliable=True
            )

        success = wait_until(
            lambda: len(box["msgs"]) == 5,
            10
        )

        assert success, "Initial messages were not delivered"

        a.stop()

        a2, _ = make(1, 6105)

        try:

            for i in range(10, 15):

                a2.send(
                    ("127.0.0.1", 6106),
                    P.CHAT,
                    struct.pack("!I", i),
                    stream=1,
                    reliable=True
                )

            success = wait_until(
                lambda: len(box["msgs"]) == 10,
                15
            )

            assert success, (
                "Messages after restart were not delivered"
            )

            assert box["msgs"] == list(range(5)) + list(range(10, 15)), (
                "Incorrect message order after restart"
            )

            print("Restart resynchronization OK")

        finally:
            a2.stop()

    finally:
        b.stop()


def test_unreachable():

    print("\nTest 4: Unreachable peer detection")

    a, box = make(1, 6107)

    try:

        a.send(
            ("127.0.0.1", 6999),
            P.CHAT,
            struct.pack("!I", 123),
            stream=1,
            reliable=True
        )

        success = wait_until(
            lambda: len(box["dead"]) > 0,
            20
        )

        assert success, "Unreachable peer was not detected"

        print("Unreachable peer detection OK")

    finally:
        a.stop()


if __name__ == "__main__":

    test_ordered_delivery_under_loss()
    test_blob_under_loss()
    test_restart_resync()
    test_unreachable()

    print("\nAll reliable tests passed")