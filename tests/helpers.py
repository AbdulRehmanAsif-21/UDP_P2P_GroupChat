import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src")
)

import protocol as P
from reliable import ReliableTransport
from membership import Membership


def wait_until(condition, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.05)
    return False


def make_peer(pid, port, port_range):
    box = {
