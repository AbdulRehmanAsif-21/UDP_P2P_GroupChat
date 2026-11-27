import os
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src")
)

import protocol as P


class TestProtocol(unittest.TestCase):

    def test_pack_unpack_message(self):
        payload = b"Hello, GroupChat!"

        packet = P.pack_message(
            P.CHAT,
            P.FLAG_RELIABLE,
            1,
            12345,
            1001,
            payload
        )

        msg = P.unpack_message(packet)

        self.assertEqual(msg.msg_type, P.CHAT)
        self.assertEqual(msg.flags, P.FLAG_RELIABLE)
        self.assertEqual(msg.stream, 1)
        self.assertEqual(msg.seq, 12345)
        self.assertEqual(msg.sender_id, 1001)
        self.assertEqual(msg.payload, payload)

    def test_empty_payload(self):
        packet = P.pack_message(
            P.HEARTBEAT,
            0,
            0,
            0,
            1001,
            b""
        )

        msg = P.unpack_message(packet)

        self.assertEqual(msg.payload, b"")
        self.assertEqual(msg.msg_type, P.HEARTBEAT)

    def test_maximum_payload(self):
        payload = b"A" * P.MAX_PAYLOAD

        packet = P.pack_message(
            P.CHUNK_DATA,
            P.FLAG_RELIABLE | P.FLAG_FIN,
            5,
            999,
            1001,
            payload
        )

        msg = P.unpack_message(packet)

        self.assertEqual(msg.payload, payload)
        self.assertEqual(len(msg.payload), P.MAX_PAYLOAD)

    def test_payload_too_large(self):
        payload = b"A" * (P.MAX_PAYLOAD + 1)

        with self.assertRaises(ValueError):
            P.pack_message(
                P.CHAT,
                0,
                1,
                0,
                1001,
                payload
            )

    def test_invalid_magic(self):
        packet = bytearray(
            P.pack_message(
                P.CHAT, 0, 1, 0, 1001, b"Test"
            )
        )

        packet[0] = 0

        with self.assertRaises(ValueError):
            P.unpack_message(bytes(packet))

    def test_invalid_version(self):
        packet = bytearray(
            P.pack_message(
                P.CHAT, 0, 1, 0, 1001, b"Test"
            )
        )

        packet[2] = 99

        with self.assertRaises(ValueError):
            P.unpack_message(bytes(packet))

    def test_invalid_packet_length(self):
        packet = P.pack_message(
            P.CHAT, 0, 1, 0, 1001, b"Test"
        )

        with self.assertRaises(ValueError):
            P.unpack_message(packet[:-1])

    def test_ack_payload(self):
        sequence = 123456789

        payload = P.pack_ack_payload(sequence)

        result = P.unpack_ack_payload(payload)

        self.assertEqual(result, sequence)

    def test_invalid_ack_payload(self):
        with self.assertRaises(ValueError):
            P.unpack_ack_payload(b"\x00\x01")

    def test_flags(self):
        flags = (
            P.FLAG_RELIABLE
            | P.FLAG_FIN
            | P.FLAG_SYN
        )

        self.assertTrue(flags & P.FLAG_RELIABLE)
        self.assertTrue(flags & P.FLAG_FIN)
        self.assertTrue(flags & P.FLAG_SYN)


if __name__ == "__main__":
    unittest.main(verbosity=2)