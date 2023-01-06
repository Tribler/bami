import numpy as np

from bami.lz.settings import PeerClockSettings

from ipv8.messaging.payload_dataclass import dataclass


@dataclass
class CompactClock:
    add: int
    seed: int
    clock: bytes


dummy_clock = CompactClock(0, 0, b'0')


class PeerClock:

    def __init__(self,
                 n_cells: int = PeerClockSettings.n_cells,
                 seed: int = 0):
        self.n_cells = n_cells
        self._clock = np.array([0] * self.n_cells, dtype=np.uint)
        self.seed = seed

    def cell_id(self, item_val: int) -> int:
        """Get cell id associated with the item"""
        return (item_val ^ self.seed) % self.n_cells

    def increment(self, item_val: int) -> int:
        """Increment the clock - adding item to the clock. Return index of updated cell"""
        c = self.cell_id(item_val)
        self._clock[c] += 1
        return c

    def compact_clock(self) -> CompactClock:
        count = min(self._clock)
        c = (self._clock - count).astype('uint16')
        return CompactClock(count, self.seed, c.tobytes())

    def __str__(self) -> str:
        return str(self._clock)

    @staticmethod
    def from_compact_clock(compact_clock: CompactClock) -> 'PeerClock':
        c = np.frombuffer(compact_clock.clock, np.uint16) + compact_clock.add
        clock = PeerClock(len(c), seed=compact_clock.seed)
        clock._clock = c.astype('uint')
        return clock

    def merge_clock(self, clock: 'PeerClock'):
        self._clock = np.maximum(self._clock, clock._clock)

    def diff(self, other_clock: 'PeerClock') -> np.array:
        return self._clock - other_clock._clock


def clocks_inconsistent(clock1: PeerClock, clock2: PeerClock) -> bool:
    """Two clocks are inconsistent with each other.
    Clocks have transactions not present """
    diff = clock2.diff(clock1)
    return np.any(diff < 0) and np.any(diff > 0)


def clock_progressive(clock1: PeerClock, clock2: PeerClock, strict: bool = True) -> bool:
    """Check if clock2 contains more than clock1"""
    clock_diff = clock2.diff(clock1)
    return np.any(clock_diff > 0) if strict else not np.any(clock_diff < 0)
