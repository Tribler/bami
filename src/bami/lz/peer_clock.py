import numpy as np

from bami.lz.settings import PeerClockSettings

from ipv8.messaging.payload_dataclass import dataclass


@dataclass
class CompactClock:
    add: int
    clock: bytes


class PeerClock:

    def __init__(self, n_cells: int = PeerClockSettings.n_cells):
        self.n_cells = n_cells
        self._clock = np.array([0] * self.n_cells, dtype=np.uint16)

    def cell_id(self, item_val: int) -> int:
        """Get cell id associated with the item"""
        return item_val % self.n_cells

    def increment(self, item_val: int):
        """Increment the clock - adding item to the clock"""
        c = self.cell_id(item_val)
        self._clock[c] += 1

    def compact_clock(self) -> CompactClock:
        count = min(self._clock)
        c = self._clock - count
        return CompactClock(count, c.tobytes())

    @staticmethod
    def from_compact_clock(compact_clock: CompactClock) -> 'PeerClock':
        c = np.frombuffer(compact_clock.clock, np.uint16) + compact_clock.add
        clock = PeerClock(len(c))
        clock._clock = c
        return c

    def diff(self, other_clock: 'PeerClock') -> np.array:
        return self._clock - other_clock


def clocks_inconsistent(clock1: PeerClock, clock2: PeerClock) -> bool:
    diff = clock2.diff(clock1)
    return np.any(diff < 0) and np.any(diff > 0)


def clock_progressive(clock1: PeerClock, clock2: PeerClock, strict: bool = False) -> bool:
    clock_diff = clock2.diff(clock1)
    return np.any(clock_diff > 0) if strict else not np.any(clock_diff < 0)
