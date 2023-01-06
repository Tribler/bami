import sys

from ipv8.messaging.serialization import default_serializer
import numpy as np

from bami.lz.sketch.peer_clock import CompactClock, PeerClock


def test_clock_packet_size():
    c = PeerClock(256, 10)
    for i in range(1, 10 ** 5):
        k = hash(str(i)) % ((sys.maxsize + 1) * 2)
        c.increment(k)
    compact = c.compact_clock()
    pack = default_serializer.pack_serializable(compact)
    unpacked_payload = default_serializer.unpack_serializable(CompactClock, pack)[0]
    c2 = PeerClock.from_compact_clock(unpacked_payload)
    assert c.n_cells == c2.n_cells
    assert np.all(c2._clock == c._clock)