from binascii import unhexlify
import random
from typing import NewType, Set, Tuple, Union

from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.peer import Peer

from bami.lz.base import BaseCommunity
from bami.lz.database.database import TransactionSyncDB
from bami.lz.payload import CompactSketch, ReconciliationRequestPayload, ReconciliationResponsePayload, \
    TransactionsRequestPayload, \
    TransactionBatchPayload, TransactionsChallengePayload, TransactionPayload
from bami.lz.settings import LZSettings
from bami.lz.sketch.bloom import BloomFilter
from bami.lz.sketch.peer_clock import clock_progressive, clocks_inconsistent, CompactClock, PeerClock
from bami.lz.reconcile import ReconciliationSetsManager
from bami.lz.utils import bytes_to_uint, get_random_string, payload_hash, uint_to_bytes

EntryHash = NewType('EntryHash', bytes)
EntrySignature = NewType('EntrySignature', bytes)


class SyncCommunity(BaseCommunity):
    """Synchronize all transactions via set reconciliation"""

    @property
    def settings(self):
        return self._settings

    @property
    def db(self) -> TransactionSyncDB:
        return self._db

    @property
    def my_peer_id(self) -> bytes:
        return self._my_peer_id

    community_id = unhexlify("6c6564676572207a65726f206973206772656174")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize item synchronization community
        """
        self._settings = kwargs.pop("settings", LZSettings())
        self._db = kwargs.pop("db", TransactionSyncDB(self._settings))
        super().__init__(*args, **kwargs)

        # Message processing
        # Gossip messages
        self.add_message_handler(TransactionPayload, self.received_transaction)
        self.add_message_handler(ReconciliationRequestPayload, self.on_received_reconciliation_request)
        self.add_message_handler(ReconciliationResponsePayload, self.on_received_reconciliation_response)
        self.add_message_handler(TransactionsChallengePayload, self.on_received_transactions_challenge)
        self.add_message_handler(TransactionsRequestPayload, self.on_received_transactions_request)
        self.add_message_handler(TransactionBatchPayload, self.on_received_transaction_batch)

        self._my_peer_id = self.my_peer.public_key.key_to_bin()
        self.reconciliation_manager = ReconciliationSetsManager(self._my_peer_id,
                                                                self._settings)

        if self.settings.start_immediately:
            self.start_tasks()

    def start_tasks(self):
        if self.settings.enable_client:
            self.start_tx_creation()
        self.start_reconciliation()

    # ----- Introduction - add to reconciliation partners
    def introduction_request_callback(self, peer, dist, payload):
        p_id = peer.public_key.key_to_bin()
        if p_id not in self.reconciliation_manager.known_partners:
            self.reconciliation_manager.initialize_new_set(p_id)
            self.reconciliation_manager.populate_with_all_known(p_id)
        super().introduction_request_callback(peer, dist, payload)

    def introduction_response_callback(self, peer, dist, payload):
        p_id = peer.public_key.key_to_bin()
        if p_id not in self.reconciliation_manager.known_partners:
            self.reconciliation_manager.initialize_new_set(p_id)
            self.reconciliation_manager.populate_with_all_known(p_id)
        super().introduction_response_callback(peer, dist, payload)

    # ------ Dummy Transaction Creation ------------
    def create_transaction(self):
        for _ in range(self.settings.tx_batch):
            script = get_random_string(self.settings.script_size)
            context = get_random_string(self.settings.script_size)
            new_tx = TransactionPayload(pk=self.my_peer_id, t_id=b'', sign=b'', script=script.encode(),
                                        context=context.encode())
            t_id = bytes_to_uint(payload_hash(new_tx), self.settings.tx_id_size)
            new_tx.t_id = uint_to_bytes(t_id)

            sign = self.crypto.create_signature(self.my_peer.key,
                                                self.prepare_packet(new_tx, sig=False))
            new_tx.sign = sign
            self.process_transaction(new_tx)

    def start_tx_creation(self):
        self.logger.debug("Starting dummy transaction creation")
        self.register_task(
            "create_transaction",
            self.create_transaction,
            interval=random.random() + self.settings.tx_freq,
            delay=random.random() + self.settings.tx_delay,
        )

    # --------- Transaction Processing ---------------

    @lazy_wrapper_unsigned(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        self.process_transaction(payload)

    def process_transaction(self, payload: TransactionPayload):

        t_id = bytes_to_uint(payload.t_id, self.settings.tx_id_size)
        self.logger.debug("Processing transaction {}".format(t_id))
        self.reconciliation_manager.populate_tx(t_id)

        if not self._db.get_tx_payload(t_id):
            self._db.add_tx_payload(t_id, payload)
            self._db.peer_clock(self.my_peer_id).increment(t_id)

    # --------------- Transaction Reconciliation -----------

    def start_reconciliation(self):
        self.logger.info("Starting reconciliation with neighbors")
        self.register_task(
            "reconciliation",
            self.reconcile_with_neighbors,
            interval=self.settings.recon_freq,
            delay=random.random() + self.settings.recon_delay,
        )

    def prepare_reconciliation_data(self, other_peer_id: bytes) -> Tuple[CompactClock, CompactSketch]:
        blm_filter = self.reconciliation_manager.get_filter(other_peer_id)
        clock = self._db.peer_clock(self.my_peer_id)
        return clock.compact_clock(), blm_filter.to_payload()

    def reconcile_with_neighbors(self):
        """Select random neighbors and reconcile if required"""
        f = self.settings.recon_fanout
        selected = random.sample(self.get_peers(), min(f, len(self.get_peers())))
        my_clock = self.db.peer_clock(self.my_peer_id)
        for p in selected:
            p_id = p.public_key.key_to_bin()
            other_clock = self.db.peer_clock(p_id)
            if clock_progressive(my_clock, other_clock):
                clock, sketch = self.prepare_reconciliation_data(p_id)
                self.send_payload(p, ReconciliationRequestPayload(clock, sketch))

    def reconcile_from_payload(self,
                               p_id: bytes,
                               payload: Union[ReconciliationResponsePayload, ReconciliationRequestPayload]) -> Set[int]:
        """Reconcile with an incoming payload and return transaction ids that peer p_id is potentially missing"""
        new_sketch = BloomFilter.from_payload(payload.sketch)
        return self.reconciliation_manager.recon_sets[p_id].reconcile(new_sketch)

    def update_peer_clock(self, p_id: bytes, new_clock: PeerClock):
        old_clock = self.db.peer_clock(p_id)
        assert not clocks_inconsistent(old_clock, new_clock)
        old_clock.merge_clock(new_clock)

    @lazy_wrapper(ReconciliationRequestPayload)
    def on_received_reconciliation_request(self,
                                           peer: Peer,
                                           payload: ReconciliationRequestPayload):
        p_id = peer.public_key.key_to_bin()
        self.logger.info("Received request from {}".format(peer))

        new_clock = PeerClock.from_compact_clock(payload.clock)
        self.update_peer_clock(p_id, new_clock)

        missing_txs = self.reconcile_from_payload(p_id, payload)
        txs_bytes = [uint_to_bytes(t, self.settings.tx_id_size) for t in missing_txs]
        txs_bytes = txs_bytes[:255]
        clock, sketch = self.prepare_reconciliation_data(p_id)
        response = ReconciliationResponsePayload(clock, sketch,
                                                 TransactionsChallengePayload(txs_bytes))
        self.send_payload(peer, response)

    @lazy_wrapper(ReconciliationResponsePayload)
    def on_received_reconciliation_response(self, peer: Peer, payload: ReconciliationResponsePayload):
        p_id = peer.public_key.key_to_bin()
        self.logger.info("Received reconciliation response from {}".format(peer))

        new_clock = PeerClock.from_compact_clock(payload.clock)
        self.update_peer_clock(p_id, new_clock)

        missing_txs = self.reconcile_from_payload(p_id, payload)
        if len(missing_txs) > 0:
            txs_bytes = [uint_to_bytes(t, self.settings.tx_id_size) for t in missing_txs]
            txs_bytes = txs_bytes[:255]
            self.send_payload(peer, TransactionsChallengePayload(txs_bytes))

    @lazy_wrapper(TransactionsChallengePayload)
    def on_received_transactions_challenge(self,
                                           peer: Peer,
                                           payload: TransactionsChallengePayload):
        self.logger.info("Received new txs from {}".format(peer))

        tx_batch_request = []
        for tid_bytes in payload.txs:
            tid = bytes_to_uint(tid_bytes, self.settings.tx_id_size)
            if not self._db.get_tx_payload(tid):
                tx_batch_request.append(tid_bytes)

        response_payload = TransactionsRequestPayload(tx_batch_request)
        self.send_payload(peer, response_payload, sig=True)

    @lazy_wrapper(TransactionsRequestPayload)
    def on_received_transactions_request(self, peer: Peer, payload: TransactionsRequestPayload):
        tx_batch = []
        for tid_bytes in payload.txs:
            tid = bytes_to_uint(tid_bytes, self.settings.tx_id_size)
            payload = self._db.get_tx_payload(tid)
            assert payload is not None

            tx_batch.append(payload)
            if len(tx_batch) == self.settings.tx_batch_size:
                response_payload = TransactionBatchPayload(tx_batch)
                self.send_payload(peer, response_payload, sig=True)
                tx_batch = []
        if len(tx_batch) > 0:
            response_payload = TransactionBatchPayload(tx_batch)
            self.send_payload(peer, response_payload, sig=True)

    @lazy_wrapper(TransactionBatchPayload)
    def on_received_transaction_batch(self, peer: Peer, payload: TransactionBatchPayload):
        v = self.prepare_packet(payload)
        if len(v) > 60000:
            self.logger.info("Received transaction {} payloads from {}. Size {}".format(len(payload.txs), peer, len(v)))
        for t in payload.txs:
            self.process_transaction(t)
