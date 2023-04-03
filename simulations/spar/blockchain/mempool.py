from dataclasses import dataclass, field
from heapq import heappop, heappush
import itertools
from typing import Any


class Mempool:
    def __init__(self, max_size: int = 10000):
        self.pq = []
        self.counter = itertools.count()

    def add(self, tx_id, tx_score):
        """Add a new transaction with a given score.
        The transaction would be selected by the maximum score"""
        count = next(self.counter)
        entry = [tx_score, count, tx_id]
        heappush(self.pq, entry)

    def pop(self) -> Any:
        """ Might raise IndexError if the heap is empty. """
        priority, count, tx_id = heappop(self.pq)
        return tx_id

    def select_top_n(self, n: int) -> list:
        """ Select the top n transactions from the mempool.
        """
        selected = []
        for _ in range(n):
            try:
                selected.append(self.pop())
            except IndexError:
                break
        return selected
