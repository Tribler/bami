from binascii import unhexlify
from collections import defaultdict
import random
from typing import Iterable, List, NewType, Set, Tuple, Union

from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.messaging.payload import IntroductionRequestPayload
from ipv8.peer import Peer

from bami.lz.base import BaseCommunity
from bami.lz.database.database import TransactionSyncDB
from bami.lz.payload import CompactMiniSketch, ReconciliationRequestPayload, ReconciliationResponsePayload, \
    TransactionBatchPayload, TransactionPayload, TransactionsChallengePayload, TransactionsRequestPayload
from bami.lz.reconcile import BloomReconciliation, MiniSketchReconciliation
from bami.lz.settings import LZSettings, SettlementStrategy, SketchAlgorithm
from bami.lz.sketch.bloom import BloomFilter
from bami.lz.sketch.minisketch import SketchError
from bami.lz.sketch.peer_clock import clock_progressive, clocks_inconsistent, CompactClock, PeerClock
from bami.lz.utils import bytes_to_uint, get_random_string, payload_hash, uint_to_bytes

EntryHash = NewType('EntryHash', bytes)
EntrySignature = NewType('EntrySignature', bytes)


class SyncCommunity(BaseCommunity):
    """Synchronize all transactions via set reconciliation"""

    @property
    def settings(self):
        return self._settings

    @property
    def memcache(self) -> TransactionSyncDB:
        return self._memcache

    @property
    def my_peer_id(self) -> bytes:
        return self._my_peer_id

    community_id = unhexlify("6c6564676572207a65726f206973206772656174")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize item synchronization community
        """
        self._settings = kwargs.pop("settings", LZSettings())
        self._memcache = kwargs.pop("memcache", TransactionSyncDB(self._settings))

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
        self.is_light_client = False
        self.connected_clients = set()

        if self._settings.sketch_algorithm == SketchAlgorithm.BLOOM:
            self.reconciliation_manager = BloomReconciliation(self._my_peer_id,
                                                              self._settings)
            self.use_bloom = True
        else:
            self.reconciliation_manager = MiniSketchReconciliation(self.my_peer_id,
                                                                   self.settings.sketch_size
                                                                   )
            self.use_bloom = False

        self.settled_txs = set()
        self.mempool_candidates = {}
        self.blocks_size = []

        self.last_reconciled_part = defaultdict(int)

        self.pending_requests = defaultdict(int)
        self.sketch_stat_has = []
        self.sketch_stat_miss = []

        if self.settings.start_immediately:
            self.start_tasks()

    def make_light_client(self):
        self.is_light_client = True

    def start_tasks(self):
        if self.settings.enable_client:
            self.start_tx_creation()
        self.start_reconciliation()

    def create_introduction_request(self, socket_address, extra_bytes=b'', new_style=False, prefix=None):
        extra_bytes = self.is_light_client.to_bytes(1, 'big')
        return super().create_introduction_request(socket_address, extra_bytes, new_style, prefix)

    def create_introduction_response(self, lan_socket_address, socket_address, identifier, introduction=None,
                                     extra_bytes=b'', prefix=None, new_style=False):
        extra_bytes = self.is_light_client.to_bytes(1, 'big')
        return super().create_introduction_response(lan_socket_address, socket_address, identifier, introduction,
                                                    extra_bytes, prefix, new_style)

    # ----- Introduction - add to reconciliation partners
    def introduction_request_callback(self, peer, dist, payload: IntroductionRequestPayload):
        p_id = peer.public_key.key_to_bin()
        is_client = bool.from_bytes(payload.extra_bytes, 'big')
        if is_client:
            self.connected_clients.add(p_id)
        else:
            if p_id not in self.reconciliation_manager.known_partners:
                self.reconciliation_manager.init_new_partner(p_id)
        super().introduction_request_callback(peer, dist, payload)

    def introduction_response_callback(self, peer, dist, payload):
        p_id = peer.public_key.key_to_bin()
        is_client = bool.from_bytes(payload.extra_bytes, 'big')
        if is_client:
            self.connected_clients.add(p_id)
        else:
            if p_id not in self.reconciliation_manager.known_partners:
                self.reconciliation_manager.init_new_partner(p_id)
        super().introduction_response_callback(peer, dist, payload)

    # ------ Transaction Creation ------------
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

            # This is a new transaction - push to neighbors
            selected = random.sample(self.get_full_nodes(),
                                     min(self.settings.initial_fanout, len(self.get_full_nodes())))
            for p in selected:
                self.logger.debug("Sending transaction to {}".format(p))
                self.ez_send(p, new_tx, sig=False)

    def get_full_nodes(self) -> Iterable[Peer]:
        return [p for p in self.get_peers() if p.public_key.key_to_bin() not in self.connected_clients]

    def start_tx_creation(self):
        if not self.is_light_client:
            self.logger.warn("Attempted to start tx production at full node")
        else:
            self.logger.debug("Starting dummy transaction creation")
            self.register_task(
                "create_transaction",
                self.create_transaction,
                interval=self.settings.tx_freq,
                delay=random.random() + self.settings.tx_delay,
            )

    # --------- Transaction Processing ---------------

    @lazy_wrapper_unsigned(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        if self.is_light_client:
            self.logger.warn("Client received transaction")
        self.process_transaction(payload)

    def on_process_new_transaction(self, t_id: int, tx_payload: TransactionPayload):
        pass

    def process_transaction(self, payload: TransactionPayload):

        t_id = bytes_to_uint(payload.t_id, self.settings.tx_id_size)

        if not self.memcache.get_tx_payload(t_id):
            self.memcache.add_tx_payload(t_id, payload)
            self.memcache.peer_clock(self.my_peer_id).increment(t_id)
            self.on_process_new_transaction(t_id, payload)
            self.reconciliation_manager.populate_tx(t_id)
        else:
            t = self.memcache.get_tx_payload(t_id)
            if t.script != payload.script:
                self.logger.warn("ID collision detected")
            else:
                self.logger.warn("Received duplicate")

    # --------------- Transaction Reconciliation -----------

    def start_reconciliation(self):
        if self.is_light_client:
            self.logger.warn("Attempted to launch full protocol at client")
        else:
            self.logger.info("Starting reconciliation with neighbors")
            self.register_task(
                "reconciliation",
                self.reconcile_with_neighbors,
                interval=self.settings.recon_freq,
                delay=random.random() + self.settings.recon_delay,
            )

    def prepare_minisketch_data(self, other_peer_id: bytes, offset: int):
        n = self.reconciliation_manager.num_sections
        sketch = self.reconciliation_manager.get_my_sketch(offset)
        clock = self.memcache.peer_clock(self.my_peer_id)
        # self.logger.info("Packing sketch {}, {}".format(offset, n))
        sketch_payload = CompactMiniSketch(o=offset, t=n, data=sketch.serialize())
        return clock.compact_clock(), 'mini', self.pack_payload(sketch_payload)

    def prepare_bloom_data(self, other_peer_id: bytes):
        blm_filter = self.reconciliation_manager.get_filter(other_peer_id)
        clock = self.memcache.peer_clock(self.my_peer_id)
        self.pack_payload(blm_filter.to_payload())
        return clock.compact_clock(), 'bloom', self.pack_payload(blm_filter.to_payload())

    def prepare_reconciliation_data(self, other_peer_id: bytes, offset: int = None) -> Tuple[CompactClock,
                                                                                             str, bytes]:
        if self._settings.sketch_algorithm == SketchAlgorithm.BLOOM:
            return self.prepare_bloom_data(other_peer_id)
        else:
            if offset is not None:
                return self.prepare_minisketch_data(other_peer_id, offset)
            data = []
            for k in range(self.reconciliation_manager.num_sections):
                data.append(self.prepare_minisketch_data(other_peer_id, k))
            return data

    def reconcile_with_neighbors(self, selected: List[Peer] = None, forced=False):
        """Select random neighbors and reconcile if required"""
        f = self.settings.recon_fanout
        full_nodes = self.get_full_nodes()
        selected = selected if selected else random.sample(full_nodes, min(f, len(full_nodes)))

        my_clock = self.memcache.peer_clock(self.my_peer_id)

        for p in selected:
            p_id = p.public_key.key_to_bin()
            other_clock = self.memcache.peer_clock(p_id)
            if forced or clock_progressive(other_clock, my_clock):
                self.logger.debug("Peer {} is not consistent. Reconcile".format(p.address))
                packs = self.prepare_reconciliation_data(p_id)
                for pack in packs:
                    clock, sk_type, sketch = pack
                    self.send_payload(p, ReconciliationRequestPayload(clock, sk_type, sketch))

    def reconcile_from_payload(self,
                               p_id: bytes,
                               payload: Union[ReconciliationResponsePayload, ReconciliationRequestPayload]) -> Set[int]:
        """Reconcile with an incoming payload and return transaction ids that peer p_id is potentially missing"""
        if self.use_bloom:
            new_sketch = BloomFilter.from_payload(payload.sketch)
            return self.reconciliation_manager.recon_sets[p_id].reconcile(new_sketch)
        else:
            new_sketch: CompactMiniSketch = self.unpack_payload(CompactMiniSketch, payload.sketch)
            if new_sketch.t > self.reconciliation_manager.num_sections:
                self.reconciliation_manager.change_num_sections(new_sketch.t)
            diff_tx = self.reconciliation_manager.reconcile(new_sketch.data, new_sketch.o, new_sketch.t)
            return diff_tx

    def update_peer_clock(self, p_id: bytes, new_clock: PeerClock):
        old_clock = self.memcache.peer_clock(p_id)
        assert not clocks_inconsistent(old_clock, new_clock)
        old_clock.merge_clock(new_clock)

    def start_periodic_settlement(self):
        self.register_task(
            "settle_transactions",
            self.settle_transactions,
            interval=self.settings.settle_freq,
            delay=random.random() + self.settings.settle_delay,
        )

    def on_settle_transactions(self, settled_txs: Iterable[int]):
        pass

    def settle_transactions(self):
        if self.settings.settle_strategy == SettlementStrategy.FAIR:
            p_id, val = min(self.mempool_candidates.items(), key=lambda x: x[1])
            all_settled = val - self.settled_txs

            cur_settled = random.sample(all_settled, min(self.settings.settle_size, len(all_settled)))
            self.blocks_size.append(len(cur_settled))
            self.settled_txs.update(cur_settled)
            self.on_settle_transactions(cur_settled)
        else:
            # Settle with all known mempool transactions
            new_settled = self.reconciliation_manager.all_txs - self.settled_txs

            cur_settled = random.sample(new_settled, min(self.settings.settle_size, len(new_settled)))
            self.blocks_size.append(len(cur_settled))
            self.settled_txs.update(cur_settled)
            self.on_settle_transactions(cur_settled)

    def reconcile_sketches(self, p_id: bytes,
                           payload: Union[ReconciliationResponsePayload,
                                          ReconciliationRequestPayload]) -> Tuple[List[int], List[int]]:
        diff_txs = self.reconcile_from_payload(p_id, payload)

        common_txs = self.reconciliation_manager.all_txs - set(diff_txs)
        self.mempool_candidates[p_id] = common_txs

        txs_to_request = set(diff_txs) - self.reconciliation_manager.all_txs
        txs_to_challenge = self.reconciliation_manager.all_txs & set(diff_txs)

        self.sketch_stat_has.append(len(txs_to_challenge))
        self.sketch_stat_miss.append(len(txs_to_request))

        txs_bytes_request = [uint_to_bytes(t, self.settings.tx_id_size) for t in txs_to_request]
        txs_bytes_request = txs_bytes_request[:255]

        txs_bytes_challenge = [uint_to_bytes(t, self.settings.tx_id_size) for t in txs_to_challenge]
        txs_bytes_challenge = txs_bytes_challenge[:255]
        return txs_bytes_request, txs_bytes_challenge

    @lazy_wrapper(ReconciliationRequestPayload)
    def on_received_reconciliation_request(self,
                                           peer: Peer,
                                           payload: ReconciliationRequestPayload):
        if self.is_light_client:
            self.logger.warn("Client Received reconciliation request")
        p_id = peer.public_key.key_to_bin()
        self.logger.debug("Received reconciliation request from {}".format(peer))

        new_clock = PeerClock.from_compact_clock(payload.clock)
        self.update_peer_clock(p_id, new_clock)

        new_sketch: CompactMiniSketch = self.unpack_payload(CompactMiniSketch, payload.sketch)

        try:
            txs_bytes_request, txs_bytes_challenge = self.reconcile_sketches(p_id, payload)
        except SketchError:
            if new_sketch.t == self.reconciliation_manager.num_sections:
                self.reconciliation_manager.change_num_sections()
            self.logger.error("Cannot reconcile sketch. Bisecting sketch. All txs {}, {}".format(
                len(self.reconciliation_manager.all_txs), self.reconciliation_manager.num_sections))
            txs_bytes_request = []
            txs_bytes_challenge = []

        vals = self.prepare_reconciliation_data(p_id, offset=new_sketch.o)
        clock, s_type, sketch = vals
        response = ReconciliationResponsePayload(clock, s_type, sketch,
                                                 TransactionsChallengePayload(txs_bytes_challenge))
        self.send_payload(peer, response)
        self.send_transaction_request(peer, txs_bytes_request)

    @lazy_wrapper(ReconciliationResponsePayload)
    def on_received_reconciliation_response(self, peer: Peer, payload: ReconciliationResponsePayload):
        p_id = peer.public_key.key_to_bin()
        self.logger.debug("Received reconciliation response from {}".format(peer))

        new_clock = PeerClock.from_compact_clock(payload.clock)
        self.update_peer_clock(p_id, new_clock)

        self.process_tx_challenge_payload(peer, payload.txs)

        try:
            txs_bytes_request, txs_bytes_challenge = self.reconcile_sketches(p_id, payload)
        except SketchError:
            new_sketch: CompactMiniSketch = self.unpack_payload(CompactMiniSketch, payload.sketch)
            self.logger.error("Cannot reconcile sketch. Bisecting sketch. All txs {}".format(
                len(self.reconciliation_manager.all_txs)))
            if new_sketch.t == self.reconciliation_manager.num_sections:
                self.reconciliation_manager.change_num_sections()
            self.reconcile_with_neighbors([peer], forced=True)
            return

        self.send_transaction_request(peer, txs_bytes_request)
        self.send_transaction_request(peer, txs_bytes_request)

    def send_transaction_request(self, peer: Peer, missing_txs: List[int]):
        new_request = [k for k in missing_txs
                       if self.pending_requests[k] < self.settings.max_pending_requests]
        if len(new_request) > 0:
            for t in new_request:
                self.pending_requests[t] += 1
            response_payload = TransactionsRequestPayload(new_request)
            self.send_payload(peer, response_payload, sig=True)

    def process_tx_challenge_payload(self, peer: Peer, payload: TransactionsChallengePayload):
        tx_batch_request = []
        for tid_bytes in payload.txs:
            tid = bytes_to_uint(tid_bytes, self.settings.tx_id_size)
            if not self.memcache.get_tx_payload(tid):
                tx_batch_request.append(tid_bytes)
        self.send_transaction_request(peer, tx_batch_request)

    @lazy_wrapper(TransactionsChallengePayload)
    def on_received_transactions_challenge(self,
                                           peer: Peer,
                                           payload: TransactionsChallengePayload):
        self.process_tx_challenge_payload(peer, payload)

    def respond_with_txs(self, peer: Peer, tx_bytes: List[bytes]):
        tx_batch = []
        for tid_bytes in tx_bytes:
            tid = bytes_to_uint(tid_bytes, self.settings.tx_id_size)
            payload = self.memcache.get_tx_payload(tid)
            assert payload is not None

            tx_batch.append(payload)
            if len(tx_batch) == self.settings.tx_batch_size:
                response_payload = TransactionBatchPayload(tx_batch)
                self.send_payload(peer, response_payload, sig=True)
                tx_batch = []
        if len(tx_batch) > 0:
            response_payload = TransactionBatchPayload(tx_batch)
            self.send_payload(peer, response_payload, sig=True)

    @lazy_wrapper(TransactionsRequestPayload)
    def on_received_transactions_request(self, peer: Peer, payload: TransactionsRequestPayload):
        """Respond to the transaction requests"""
        self.logger.debug("Received new tx ids from {}".format(peer))
        self.respond_with_txs(peer, payload.txs)

    @lazy_wrapper(TransactionBatchPayload)
    def on_received_transaction_batch(self, peer: Peer, payload: TransactionBatchPayload):
        """Process received transactions sent in a batch"""
        v = self.prepare_packet(payload)
        if len(v) > 60000:
            self.logger.info("Received transaction {} payloads from {}. Size {}".format(len(payload.txs), peer, len(v)))
        for t in payload.txs:
            self.process_transaction(t)
