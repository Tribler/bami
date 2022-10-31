import random
from asyncio import get_event_loop
from binascii import hexlify, unhexlify
from collections import defaultdict
from typing import Tuple, NewType

from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.messaging.interfaces.udp.endpoint import UDPv4Address
from ipv8.peer import Peer
from ipv8.types import Payload

from bami.peerreview.database import TamperEvidentLog, PeerTxDB, EntryType
from bami.peerreview.payload import LogEntryPayload, TransactionPayload, TxsChallengePayload, TxId, TxsRequestPayload, \
    TxsProofPayload, LoggedMessagePayload, LoggedAuthPayload
from bami.peerreview.settings import PeerReviewSettings
from bami.peerreview.utils import get_random_string, payload_hash

EntryHash = NewType('EntryHash', bytes)
EntrySignature = NewType('EntrySignature', bytes)


class PeerReviewCommunity(Community):
    community_id = unhexlify("a42c847a628e1414cffb6a4626b7fa0999fba888")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the Basalt community and required variables.
        """
        self.settings = kwargs.pop("settings", PeerReviewSettings())
        super().__init__(*args, **kwargs)

        self.pr_logs = defaultdict(lambda: TamperEvidentLog())
        self.log_auth = defaultdict(lambda: {})

        self.known_peer_txs = PeerTxDB()

        # Message state machine
        # Gossip messages
        self.add_message_handler(TransactionPayload, self.received_transaction)
        self.add_message_handler(TxsChallengePayload, self.received_txs_challenge)
        self.add_message_handler(TxsProofPayload, self.received_txs_proof)
        self.add_message_handler(TxsRequestPayload, self.received_tx_request)

        # PeerReview messages
        self.add_message_handler(LogEntryPayload, self.received_log_entry)
        self.add_message_handler(LoggedMessagePayload, self.received_logged_message)
        self.add_message_handler(LoggedAuthPayload, self.received_log_authenticator)

        self.my_peer_id = self.my_peer.public_key.key_to_bin()

        if self.settings.start_immediately:
            self.start_reconciliation()
            self.start_tx_creation()

    def start_tasks(self):
        print("The number of peers i'm connected to ", len(self.get_peers()))
        self.start_reconciliation()
        self.start_tx_creation()

    def create_new_log_entry(self, entry_type: EntryType, cp_pk: bytes, cp_sn: int, msg_hash: bytes) -> \
            Tuple[LogEntryPayload, EntryHash, EntrySignature]:
        new_entry = self.pr_logs[self.my_peer_id].create_new_entry(self.my_peer_id,
                                                                   entry_type,
                                                                   cp_pk,
                                                                   cp_sn,
                                                                   msg_hash)
        entry_hash = payload_hash(new_entry)
        signature = self.crypto.create_signature(self.my_peer.key, entry_hash)

        auth_payload = LoggedAuthPayload(new_entry.pk, new_entry.sn, entry_hash, signature)
        self.log_auth[self.my_peer_id][new_entry.sn] = auth_payload

        return new_entry, entry_hash, signature

    def logged_push(self, p: Peer, payload: Payload):
        msg_hash = payload_hash(payload)
        new_entry, entry_hash, signature = self.create_new_log_entry(EntryType.SEND,
                                                                     p.public_key.key_to_bin(),
                                                                     0,
                                                                     msg_hash
                                                                     )
        logged_msg_payload = LoggedMessagePayload(self.my_peer_id, new_entry.sn, new_entry.p_hash,
                                                  signature, payload, entry_hash
                                                  )
        self.ez_send(p, logged_msg_payload, sig=False)

    def random_push(self, payload: Payload):
        f = min(self.settings.fanout, len(self.get_peers()))
        selected = random.sample(self.get_peers(), f)
        for p in selected:
            self.logged_push(p, payload)

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
            delay=random.random() + self.settings.tx_delay,
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
            self.ez_send(p, request, sign=False)

    @lazy_wrapper(TxsChallengePayload)
    def received_txs_challenge(self, p: Peer, payload: TxsChallengePayload):

        self.logger.info("{} Received transactions challenge from peer {}".format(get_event_loop().time(), p))

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
        self.logger.info("{} Received transactions request from peer {}".format(get_event_loop().time(), p))
        for t in payload.tx_ids:
            tx_payload = self.known_peer_txs.get_tx_payload(t.tx_id)
            self.logged_push(p, tx_payload)

    @lazy_wrapper(LogEntryPayload)
    def received_log_entry(self, p: Peer, payload: LogEntryPayload):
        self.logger.info("{} Received log entry from peer {}".format(get_event_loop().time(), p))

    @lazy_wrapper(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        self.process_transaction(p, payload)

    def process_transaction(self, p: Peer, payload: TransactionPayload):
        p_id = p.public_key.key_to_bin()
        tx_id = payload_hash(payload)
        self.known_peer_txs.add_tx_payload(tx_id, payload)

        self.logger.info("{} Processing transaction {}".format(get_event_loop().time(), hexlify(tx_id)))


        self.known_peer_txs.add_peer_tx(
            p_id, tx_id)
        self.known_peer_txs.add_peer_tx(self.my_peer_id, tx_id)

    @lazy_wrapper(TxsProofPayload)
    def received_txs_proof(self, p: Peer, payload: TxsProofPayload):
        self.logger.info("{} Received transactions proofs from peer {}".format(get_event_loop().time(), p))
        p_id = p.public_key.key_to_bin()
        for t in payload.tx_ids:
            self.known_peer_txs.add_peer_tx(p_id, t.tx_id)

    @lazy_wrapper_unsigned(LoggedMessagePayload)
    def received_logged_message(self, p_address: UDPv4Address, payload: LoggedMessagePayload):
        self.logger.info("{} Received logged message from {}".format(get_event_loop().time(), p_address))
        auth_payload = LoggedAuthPayload(payload.pk, payload.sn, payload.lh, payload.sign)
        self.log_auth[payload.pk][payload.sn] = auth_payload
        peer = Peer(payload.pk, p_address)

        self.process_transaction(peer, payload.msg)
        # Respond with acknowledgement
        self.acknowledge_message(p_address, payload)

    def acknowledge_message(self, p: UDPv4Address, payload: LoggedMessagePayload):

        new_entry, entry_hash, entry_sign = self.create_new_log_entry(EntryType.RECEIVE, payload.pk, payload.sn,
                                                                      payload_hash(payload.msg))
        # Respond with auth for the log
        auth_payload = self.log_auth[self.my_peer_id][new_entry.sn]

        self._ez_senda(p, auth_payload, sig=False)

    @lazy_wrapper_unsigned(LoggedAuthPayload)
    def received_log_authenticator(self, p: UDPv4Address, payload: LoggedAuthPayload):
        self.logger.info("{} Received log authenticator from {}".format(get_event_loop().time(), p))
        self.log_auth[payload.pk][payload.sn] = payload
