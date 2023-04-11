from dataclasses import dataclass, field
from heapq import heappop, heappush
import itertools
from typing import Any, Tuple, List


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

    def remove(self, tx_id):
        """Remove a transaction from the mempool"""
        for entry in self.pq:
            if entry[2] == tx_id:
                self.pq.remove(entry)
                break

    def pop(self) -> Any:
        """ Might raise IndexError if the heap is empty. """
        priority, count, tx_id = heappop(self.pq)
        return priority, tx_id

    def select_top_n(self, n: int) -> tuple[list[bytes], list[int]]:
        """ Select the top n transactions from the mempool.
        """
        selected = []
        selected_fees = []
        for _ in range(n):
            try:
                priority, tx_id = self.pop()
                selected.append(tx_id)
                selected_fees.append(priority)
            except IndexError:
                break
        return selected, selected_fees
