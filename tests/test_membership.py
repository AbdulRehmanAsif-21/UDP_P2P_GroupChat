import os
import sys

sys.path.insert(
    0,
    os.path.dirname(__file__)
)

from helpers import make_peer, wait_until

PORTS = range(6200, 6204)


def test_join_leave_crash():
    peers = []
    try:
        # Create three peers
        p1 = make_peer(1, 6201, PORTS)
        peers.append(p1)

        p2 = make_peer(2, 6202, PORTS)
        peers.append(p2)

        p3 = make_peer(3, 6203, PORTS)
        peers.append(p3)

        # First peer: nobody answers
        assert not p1.mem.join(timeout=1.0)

        # Second peer joins
