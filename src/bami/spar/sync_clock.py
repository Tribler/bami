import numpy as np

from bami.spar.payload import CompactClock

dummy_clock = CompactClock(add=0, data=b'0')


class SyncClock:
    CSUM_BITS = 32

    def __init__(self,
                 n_cells: int = 32):
        self.n_cells = n_cells
        self.data = np.array([0] * self.n_cells, dtype=np.int64)
        self.seed = 0
        self.csum = [0] * self.n_cells
        self.max_div = 2 ** SyncClock.CSUM_BITS

    def cell_id(self, item_val: int) -> int:
        """Get cell id associated with the item"""
        return (item_val ^ self.seed) % self.n_cells

    def increment(self, item_val: int) -> int:
        """Increment the data - adding item to the data. Return index of updated cell"""
        c = self.cell_id(item_val)
        self.csum[c] = self.csum[c] ^ (item_val % self.max_div)
        self.data[c] += 1
        return c

    def increase(self, item_val: int, amount: int) -> int:
        """Increase the data - adding item to the data. Return index of updated cell"""
        c = self.cell_id(item_val)
        self.csum[c] = self.csum[c] ^ (item_val % self.max_div)
        self.data[c] += amount
        return c

    def compact_clock(self) -> CompactClock:
        count = min(self.data)
        c = (self.data - count).astype('uint16')
        return CompactClock(add=count, data=c.tobytes())

    def __str__(self) -> str:
        return str(self.data)

    @staticmethod
    def from_compact_clock(compact_clock: CompactClock) -> 'SyncClock':
        c = np.frombuffer(compact_clock.data, np.uint16) + compact_clock.add
        clock = SyncClock(len(c))
        clock.data = c.astype('uint')
        return clock

    def merge_clock(self, clock: 'SyncClock'):
        self.data = np.maximum(self.data, clock.data)

    def diff(self, other_clock: 'SyncClock') -> np.array:
        return self.data - other_clock.data


class ClockTable(SyncClock):
    def __init__(self, n_cells: int = 32) -> object:
        super().__init__(n_cells)

        self.item_cells = [set() for _ in range(n_cells)]

    def increment(self, item_val: int) -> int:
        v = super().increment(item_val)
        self.item_cells[v].add(item_val)

    def sorted_diff(self, other_clock: 'SyncClock'):
        diff = self.diff(other_clock)
        sorted_indices = np.argsort(diff)[::-1]
        for i in sorted_indices:
            if diff[i] > 0:
                for item in self.item_cells[i]:
                    yield item


def clocks_inconsistent(clock1: SyncClock, clock2: SyncClock) -> bool:
    """Two clocks are inconsistent with each other.
    Clocks have transactions not present """
    diff = clock2.diff(clock1)
    return np.any(diff < 0) and np.any(diff > 0)


def clock_progressive(clock1: SyncClock, clock2: SyncClock, strict: bool = True) -> bool:
    """Check if clock2 contains more than clock1"""
    clock_diff = clock2.diff(clock1)
    return np.any(clock_diff > 0) if strict else not np.any(clock_diff < 0)
