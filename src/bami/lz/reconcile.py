import random

from bami.lz.base import BaseMixin
from bami.lz.payload import TxsChallengePayload


class Reconciliation(BaseMixin):

    def start_reconciliation(self):
        self.register_task(
            "reconciliation",
            self.reconcile_with_neighbors,
            interval=self.settings.recon_freq,
            delay=random.random() + self.settings.recon_delay,
        )

    def reconcile_with_neighbors(self):
        my_state = self.peer_db.get_peer_txs(self.my_peer_id)
        f = self.settings.recon_fanout
        selected = random.sample(self.get_peers(), min(f, len(self.get_peers())))
        for p in selected:
            p_id = p.public_key.key_to_bin()
            peer_state = self.peer_db.get_peer_txs(p_id)
            set_diff = my_state - peer_state
            if len(set_diff) > 0:
                request = TxsChallengePayload([TxId(s) for s in set_diff])
                self.ez_send(p, request, sign=False)


    # Reconcilation procedure:
    # 1. Transactions are put to
    # What is there is block restructuring? - There is a change in the transaction inclusion?
    # Can be two transaction sets - all seen vs valid.
    # The transaction sets are - clock and pairwise filter.
    # With the filter get a checksum for each corresponding cell.

    # CellState
    # BloomFilter
