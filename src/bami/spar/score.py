from collections import defaultdict
from typing import Any, Dict, Tuple

"""
A module to score peers based on the amount of work they have done.
The work is calculated based on the received and sent messages signed by the peer. 
"""


class LocalCounterScore:

    def __init__(self):
        self.scores = defaultdict(lambda: 0)

    def get_score(self, peer_id: bytes) -> int:
        """
        Get the score for a given peer.
        """
        return self.scores

    def update(self, peer_id: bytes, sent: int, received: int) -> None:
        """
        Update the counters for a given peer.
        """
        self._counters[peer_id] = (sent, received)

    def on_receive(self, peer_id: bytes, packet: Any) -> None:
        """
        Update the counters for a given peer.
        """
        sent, received = self._counters[peer_id]
        self._counters[peer_id] = (sent, received + 1)

    def on_send(self, peer_id: bytes, packet: Any) -> None:
        """
        Update the counters for a given peer.
        """
        sent, received = self._counters[peer_id]
        self._counters[peer_id] = (sent + 1, received)
