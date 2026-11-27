"""Peer discovery and membership: JOIN, JOIN_ACK, HEARTBEAT, LEAVE."""
import json
import threading
import time
import traceback

import protocol as P

DEFAULT_PORT_RANGE = range(5000, 5011)
HEARTBEAT_INTERVAL = 1.0
DEAD_AFTER = 4.0


class Membership:
    def __init__(
        self,
        my_id,
        port,
        name,
        transport,
        on_peer_joined,
        on_peer_dead,
        port_range=DEFAULT_PORT_RANGE,
        heartbeat_interval=HEARTBEAT_INTERVAL,
        dead_after=DEAD_AFTER
    ):
        self.my_id = my_id
        self.port = port
        self.name = name
        self.transport = transport
