from asyncio import get_event_loop
from binascii import unhexlify
import random
from collections import defaultdict

from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.types import Payload, Peer

from bami.peerreview.database import EntryType, TamperEvidentLog, PeerTxDB
from bami.peerreview.payload import LogEntryPayload, TransactionPayload, TxsChallengePayload, TxId, TxsRequestPayload, \
    TxsProofPayload
from bami.peerreview.settings import PeerReviewSettings
from bami.peerreview.utils import get_random_string, payload_hash


class PeerReviewCommunity(Community):
    community_id = unhexlify("a42c847a628e1414cffb6a4626b7fa0999fba888")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the Basalt community and required variables.
        """
        self.settings = kwargs.pop("settings", PeerReviewSettings())
        super().__init__(*args, **kwargs)

        self.pr_logs = defaultdict(lambda: TamperEvidentLog())
        self.known_peer_txs = PeerTxDB()

        # Message state machine
        self.add_message_handler(TransactionPayload, self.received_transaction)
        self.add_message_handler(TxsChallengePayload, self.received_txs_challenge)
        self.add_message_handler(TxsProofPayload, self.received_txs_proof)
        self.add_message_handler(TxsRequestPayload, self.received_tx_request)

        self.my_peer_id = self.my_peer.public_key.key_to_bin()

        self.start_reconciliation()
        self.start_tx_creation()

    def random_push(self, payload: Payload):
        f = min(self.settings.fanout, len(self.get_peers()))
        selected = random.sample(self.get_peers(), f)
        for p in selected:
            self.ez_send(p, payload)

    # Client routines
    def create_transaction(self):
        script = get_random_string(self.settings.script_size)
        new_tx = TransactionPayload(script.encode())
        tx_hash = payload_hash(new_tx)

        self.known_peer_txs.add_peer_tx(self.my_peer_id, tx_hash)
        self.known_peer_txs.add_tx_payload(tx_hash, new_tx)

        # Initial push to the network
        self.random_push(new_tx)

    def start_tx_creation(self):
        self.register_task(
            "create_transaction",
            self.create_transaction,
            interval=random.random() + self.settings.tx_freq,
            delay=random.random(),
        )

    # ---- Community audit routines
    def start_reconciliation(self):
        self.register_task(
            "reconciliation",
            self.reconcile_with_neighbors,
            interval=self.settings.recon_freq,
            delay=self.settings.recon_delay,
        )

    def reconcile_with_neighbors(self):
        my_state = self.known_peer_txs.get_peer_txs(self.my_peer_id)
        f = self.settings.recon_fanout
        selected = random.sample(self.get_peers(), min(f, len(self.get_peers())))
        for p in selected:
            p_id = p.public_key.key_to_bin()
            peer_state = self.known_peer_txs.get_peer_txs(p_id)
            set_diff = my_state - peer_state

            request = TxsChallengePayload([TxId(s) for s in set_diff])
            self.ez_send(p, request)

    @lazy_wrapper(TxsChallengePayload)
    def received_txs_challenge(self, p: Peer, payload: TxsChallengePayload):

        self.logger.debug("{} Received transactions challenge from peer {}".format(get_event_loop().time(), p))

        my_state = self.known_peer_txs.get_peer_txs(self.my_peer_id)
        to_request = []
        to_prove = []

        for t in payload.tx_ids:
            if t.tx_id not in my_state:
                to_request.append(t)
            else:
                to_prove.append(t)

        if len(to_request) > 0:
            request = TxsRequestPayload([t for t in to_request])
            self.ez_send(p, request)

        if len(to_prove):
            proof = TxsProofPayload([t for t in to_request])
            self.ez_send(p, proof)

    @lazy_wrapper(TxsRequestPayload)
    def received_tx_request(self, p: Peer, payload: TxsRequestPayload):
        self.logger.debug("{} Received transactions request from peer {}".format(get_event_loop().time(), p))
        for t in payload.tx_ids:
            tx_payload = self.known_peer_txs.get_tx_payload(t.tx_id)
            self.ez_send(p, tx_payload)
            
    @lazy_wrapper(LogEntryPayload)
    def received_log_entry(self, p: Peer, payload: LogEntryPayload):
        self.logger.debug("{} Received log entry from peer {}".format(get_event_loop().time(), p))

        # names = ["pk", "sn", "is_send", "p_hash", "cp_pk", "cp_sn", "varlenH"]
        # payload.pk - public key of the logger
        # payload.sn - seq number of the logger
        # payload.is_send - send or receive entry
        # payload.p_hash  - previous hash of the log entry
        # payload.cp_pk  - counter-party public key
        # payload.cp_sn - counter-party sequence number
        # payload.msg - the message recorded at the log entry.
        pass

    @lazy_wrapper(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        self.logger.debug("{} Received log entry from peer {}".format(get_event_loop().time(), p))
        p_id = p.public_key.key_to_bin()
        tx_id = payload_hash(payload)
        self.known_peer_txs.add_tx_payload(tx_id, payload)

        self.known_peer_txs.add_peer_tx(
            p_id, tx_id)
        self.known_peer_txs.add_peer_tx(self.my_peer_id, tx_id)

    @lazy_wrapper(TxsProofPayload)
    def received_txs_proof(self, p: Peer, payload: TxsProofPayload):
        self.logger.debug("{} Received transactions proofs from peer {}".format(get_event_loop().time(), p))
        p_id = p.public_key.key_to_bin()
        for t in payload.tx_ids:
            self.known_peer_txs.add_peer_tx(p_id, t.tx_id)
