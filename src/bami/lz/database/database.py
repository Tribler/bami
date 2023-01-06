from collections import defaultdict
from typing import Any, Iterable, Optional, Set

from ipv8.types import Payload

from bami.lz.settings import LZSettings
from bami.lz.sketch.peer_clock import PeerClock


class TransactionSyncDB:
    """Storage for transactions and peers that sync transactions"""

    def __init__(self, settings: LZSettings):
        self.tx_payloads = {}
        self.peer_txs = defaultdict(lambda: set())

        self.peer_clocks = defaultdict(lambda: PeerClock(settings.n_cells))

    def add_tx_payload(self, tx_id: Any, tx_payload: Payload):
        self.tx_payloads[tx_id] = tx_payload

    def get_tx_payload(self, tx_id: Any) -> Optional[Payload]:
        return self.tx_payloads.get(tx_id, None)

    def peer_has_transaction(self, peer_id, tx_id: int):
        return tx_id in self.peer_txs[peer_id]

    def add_peer_tx(self, p_id: Any, tx_id: Any):
        self.peer_txs[p_id].add(tx_id)

    def get_peer_txs(self, p_id: Any) -> Set[Any]:
        return self.peer_txs[p_id]

    def get_tx_set_diff(self, p_id, other_p_id) -> Iterable[Any]:
        """Get set difference S(p_id) - S(other_p_id)"""
        return self.get_peer_txs(p_id) - self.get_peer_txs(other_p_id)

    def peer_clock(self, peer_id: Any) -> PeerClock:
        return self.peer_clocks[peer_id]
