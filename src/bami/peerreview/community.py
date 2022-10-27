import random
from collections import defaultdict

from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.types import Peer

from bami.peerreview.database import TamperEvidentLog, PeerTxDB
from bami.peerreview.payload import TransactionPayload, TxsChallengePayload, TxId, TxsRequestPayload, TxsProofPayload
from bami.peerreview.settings import PeerReviewSettings
from bami.peerreview.utils import get_random_string, payload_hash


class PeerReviewCommunity(Community):

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the Basalt community and required variables.
        """
        self.settings = kwargs.pop("settings", PeerReviewSettings())
        super().__init__(*args, **kwargs)

        self.logs = defaultdict(lambda: TamperEvidentLog())
        self.known_peer_txs = PeerTxDB()

        # Message state machine
        # self.add_message_handler(PullPayload, self.received_pull)
        # self.add_message_handler(PushPayload, self.received_push)

        self.my_peer_id = self.my_peer.public_key.key_to_bin()

    # Client routines
    def create_transaction(self):
        script = get_random_string(self.settings.script_size)
        new_tx = TransactionPayload(script)
        tx_hash = payload_hash(new_tx)

        self.known_peer_txs.add_peer_tx(self.my_peer_id, tx_hash)
        self.known_peer_txs.add_tx_payload(tx_hash, new_tx)

        # Initial push to the network

    def start_tx_creation(self):
        self.register_task(
            "create_transaction",
            self.create_transaction(),
            interval=self.settings.tx_freq,
            delay=0,
        )

    # ---- Community audit routines
    def start_reconciliation(self):
        self.register_task(
            "create_transaction",
            self.reconcile_with_neighbors(),
            interval=self.settings.recon_freq,
            delay=0,
        )

    def reconcile_with_neighbors(self):
        my_state = self.known_peer_txs.get_peer_txs(self.my_peer_id)
        f = self.settings.recon_fanout
        selected = random.sample(self.get_peers(), f)
        for p in selected:
            p_id = p.public_key.key_to_bin()
            peer_state = self.known_peer_txs.get_peer_txs(p_id)
            set_diff = my_state - peer_state

            request = TxsChallengePayload([TxId(s) for s in set_diff])
            self.ez_send(p, request)

    @lazy_wrapper(TxsChallengePayload)
    def received_txs_challenge(self, p: Peer, payload: TxsChallengePayload):

        my_state = self.known_peer_txs.get_peer_txs(self.my_peer_id)
        to_request = []
        to_prove = []

        for t in payload.tx_ids:
            if t not in my_state:
                to_request.append(t)
            else:
                to_prove.append(t)

        if len(to_request) > 0:
            request = TxsRequestPayload([TxId(t) for t in to_request])
            self.ez_send(p, request)

        if len(to_prove):
            proof = TxsProofPayload([TxId(t) for t in to_request])
            self.ez_send(p, proof)

    @lazy_wrapper(TxsRequestPayload)
    def received_tx_request(self, p: Peer, payload: TxsRequestPayload):
        for tx_id in payload.tx_ids:
            tx_payload = self.known_peer_txs.get_tx_payload(tx_id)
            self.ez_send(p, tx_payload)

    @lazy_wrapper(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        tx_id = payload_hash(payload)
        self.known_peer_txs.add_tx_payload(tx_id, payload)
        p_id = p.public_key.key_to_bin()
        self.known_peer_txs.add_peer_tx(p_id, tx_id)
        self.known_peer_txs.add_peer_tx(self.my_peer_id, tx_id)

    @lazy_wrapper(TxsProofPayload)
    def received_txs_proof(self, p: Peer, payload: TxsProofPayload):
        p_id = p.public_key.key_to_bin()
        for t_id in payload.tx_ids:
            self.known_peer_txs.add_peer_tx(p_id, t_id)
