import random
from asyncio import get_event_loop
from binascii import unhexlify
from collections import defaultdict
from typing import List

from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.payload import IntroductionRequestPayload
from ipv8.types import Peer, AnyPayload

from bami.broadcast.payload import TransactionPayload, TxBatchPayload, BatchAckPayload, HeaderPayload, \
    BatchRequestPayload
from bami.broadcast.settings import MempoolBroadcastSettings
from bami.common.network import SimulatedNetwork
from bami.lz.utils import get_random_string, payload_hash


class MempoolBroadcastCommunity(Community):
    community_id = unhexlify("16a5bb5b7d9f7848ba2398db0ec1909cf207613b")

    def __init__(self, *args, **kwargs) -> None:
        self.settings = kwargs.pop("settings", MempoolBroadcastSettings())
        super().__init__(*args, max_peers=1000, **kwargs)

        self.latency_sim = self.settings.simulate_network_latency
        self.pending_transactions = []
        self.batches = {}

        self.my_peer_id = self.my_peer.public_key.key_to_bin()

        if self.latency_sim:
            self.sim_net = SimulatedNetwork()
            self.sim_net.fix_location(self.my_peer_id)

        self.is_transaction_creator = False

        self.connected_clients = set()

        self.batch_confirms = defaultdict(set)
        self.batch_finalized = set()

        self.header_round = 1
        self.batch_round = 1

        self.pending_batch_requests = {}
        self.awaited_headers = {}
        self.pending_headers = {}
        self.batch_to_header = {}

        self.receive_counter = 0
        self.send_counter = 0

        # Message state machine
        self.add_message_handler(TxBatchPayload, self.receive_new_batch)
        self.add_message_handler(BatchAckPayload, self.receive_batch_ack)
        self.add_message_handler(HeaderPayload, self.receive_header)
        self.add_message_handler(BatchRequestPayload, self.receive_batch_request)
        self.add_message_handler(TransactionPayload, self.received_transaction)

    def on_packet(self, packet, warn_unknown=True):
        self.receive_counter += len(packet)
        super().on_packet(packet, warn_unknown)

    def ezr_pack(self, msg_num: int, *payloads: AnyPayload, **kwargs) -> bytes:
        self.send_counter += len(payloads)
        return super().ezr_pack(msg_num, *payloads, **kwargs)

    def ez_send(self, peer: Peer, *payloads: AnyPayload, **kwargs) -> None:
        if self.latency_sim:
            latency = self.sim_net.get_link_latency(self.my_peer_id, peer.public_key.key_to_bin())
            self.register_anonymous_task("send",
                                         super().ez_send,
                                         peer, *payloads, **kwargs,
                                         delay=latency)
        else:
            super().ez_send(peer, *payloads, **kwargs)

    def create_introduction_request(self, socket_address, extra_bytes=b'', new_style=False, prefix=None):
        extra_bytes = self.is_transaction_creator.to_bytes(1, 'big')
        return super().create_introduction_request(socket_address, extra_bytes, new_style, prefix)

    def create_introduction_response(self, lan_socket_address, socket_address, identifier, introduction=None,
                                     extra_bytes=b'', prefix=None, new_style=False):
        extra_bytes = self.is_transaction_creator.to_bytes(1, 'big')
        return super().create_introduction_response(lan_socket_address, socket_address, identifier, introduction,
                                                    extra_bytes, prefix, new_style)

    def introduction_request_callback(self, peer, dist, payload: IntroductionRequestPayload):
        p_id = peer.public_key.key_to_bin()
        is_client = bool.from_bytes(payload.extra_bytes, 'big')
        if is_client:
            self.connected_clients.add(p_id)
        if self.latency_sim:
            self.sim_net.fix_location(p_id)
        super().introduction_request_callback(peer, dist, payload)

    def introduction_response_callback(self, peer, dist, payload):
        p_id = peer.public_key.key_to_bin()
        is_client = bool.from_bytes(payload.extra_bytes, 'big')
        if is_client:
            self.connected_clients.add(p_id)
        if self.latency_sim:
            self.sim_net.fix_location(p_id)
        super().introduction_response_callback(peer, dist, payload)

    def get_full_nodes(self) -> List[Peer]:
        return [p for p in self.get_peers() if p.public_key.key_to_bin() not in self.connected_clients]

    def create_transaction(self):
        script = get_random_string(self.settings.script_size).encode()
        new_tx = TransactionPayload(b"", script)
        new_tx.tx_id = payload_hash(new_tx)
        self.on_transaction_created(new_tx)

        # This is a new transaction - push to neighbors
        full_nodes = self.get_full_nodes()
        selected = random.sample(full_nodes,
                                 min(self.settings.initial_fanout, len(full_nodes)))
        for p in selected:
            self.ez_send(p, new_tx)

    @lazy_wrapper(TransactionPayload)
    def received_transaction(self, p: Peer, tx: TransactionPayload):
        """Receive new transaction"""
        self.feed_batch_maker(tx)

    def start_tx_creation(self):
        self.register_task(
            "create_transaction",
            self.create_transaction,
            interval=self.settings.tx_freq,
            delay=self.settings.tx_delay,
        )

    def on_transaction_created(self, new_tx: TransactionPayload):
        pass

    def feed_batch_maker(self, new_tx: TransactionPayload):
        self.pending_transactions.append(new_tx)
        if len(self.pending_transactions) >= self.settings.batch_size:
            self.seal_new_batch()

    def start_batch_making(self):
        self.register_task(
            "batch_maker",
            self.seal_new_batch,
            interval=self.settings.batch_freq,
            delay=random.random() * self.settings.batch_delay,
        )

    def on_new_batch_created(self, new_batch: TxBatchPayload):
        pass

    def seal_new_batch(self):
        """Sign and broadcast batch"""
        if len(self.pending_transactions) > 0:
            # select first k transactions for the batch
            selected = self.pending_transactions[:self.settings.batch_size]
            batch = TxBatchPayload(self.my_peer.public_key.key_to_bin(), self.batch_round, selected)
            self.batch_round += 1
            self.on_new_batch_created(new_batch=batch)
            wait_id = self.broadcast(batch)
            # Store batch
            self.batches[wait_id] = batch
            # remove selected from pending
            self.pending_transactions = self.pending_transactions[self.settings.batch_size:]

    def broadcast(self, message_payload) -> bytes:
        """Broadcast message to all peers and return the awaited id for acknowledgement"""
        for p in self.get_full_nodes():
            self.ez_send(p, message_payload)
        return payload_hash(message_payload)

    def lucky_broadcast(self, message_payload, num_nodes=None):
        """Broadcast message to all peers and return the awaited id for acknowledgement"""
        sample_size = num_nodes if num_nodes else self.settings.sync_retry_nodes
        lucky_peers = random.sample(self.get_full_nodes(), sample_size)
        for peer in lucky_peers:
            self.ez_send(peer, message_payload)

    @lazy_wrapper(TxBatchPayload)
    def receive_new_batch(self, p: Peer, batch: TxBatchPayload):
        """Receive new batch"""
        batch_id = payload_hash(batch)
        if batch_id not in self.batches:
            # store the batch and send ack
            self.batches[batch_id] = batch
            # pop is we are waiting for pending batch
            if batch_id in self.pending_batch_requests:
                self.pending_batch_requests.pop(batch_id)
                header_id = self.batch_to_header[batch_id]
                self.awaited_headers[header_id].remove(batch_id)
                if len(self.awaited_headers[header_id]) == 0:
                    self.on_new_header(self.pending_headers.pop(header_id))

            self.ez_send(p, BatchAckPayload(batch_id))

    @lazy_wrapper(BatchAckPayload)
    def receive_batch_ack(self, p: Peer, ack: BatchAckPayload):
        """Receive batch ack"""
        self.batch_confirms[ack.batch_id].add(p.public_key.key_to_bin())

    def start_header_making(self):
        self.register_task(
            "header_maker",
            self.make_new_header,
            interval=self.settings.header_freq,
            delay=random.random() * self.settings.header_delay,
        )

    def make_new_header(self):
        batch_ids = []
        for batch_id in self.batch_confirms.keys():
            if (batch_id not in self.batch_finalized and
                    len(self.batch_confirms[batch_id]) >= self.settings.quorum_threshold):
                batch_ids.append(BatchAckPayload(batch_id))
                self.batch_finalized.add(batch_id)
        if len(batch_ids) > 0:
            header = HeaderPayload(self.my_peer.public_key.key_to_bin(), self.header_round, batch_ids)
            self.header_round += 1
            self.broadcast(header)
            self.on_new_header(header)

    def on_new_header(self, new_header: HeaderPayload):
        for batch in new_header.batches:
            batch_id = batch.batch_id
            batch_obj = self.batches[batch_id]
            for tx in batch_obj.txs:
                self.on_transaction_finalized(tx)

    def on_transaction_finalized(self, tx: TransactionPayload):
        pass

    @lazy_wrapper(HeaderPayload)
    def receive_header(self, p: Peer, header: HeaderPayload):
        """Receive new header"""
        missing_batches = [b_obj for b_obj in header.batches if b_obj.batch_id not in self.batches]
        if len(missing_batches) > 0:
            self.ez_send(p, BatchRequestPayload(missing_batches))
            # Add time to the missing batches
            header_id = payload_hash(header)
            for batch_obj in missing_batches:
                batch_id = batch_obj.batch_id
                self.pending_batch_requests[batch_id] = get_event_loop().time()
                self.batch_to_header[batch_id] = header_id

            self.awaited_headers[header_id] = set([b_obj.batch_id for b_obj in missing_batches])
            self.pending_headers[header_id] = header
        else:
            # trigger on new header
            self.on_new_header(header)

    def sync_timer(self):
        """Check for missing batches and request them"""
        missing_batches = []
        for batch_id, time in self.pending_batch_requests.items():
            if get_event_loop().time() - time > self.settings.sync_retry_time:
                missing_batches.append(BatchAckPayload(batch_id))
        if len(missing_batches) > 0:
            self.lucky_broadcast(BatchRequestPayload(missing_batches))

    def start_sync_timer(self):
        self.register_task(
            "sync_timer",
            self.sync_timer,
            interval=self.settings.sync_timer_delta,
            delay=random.random() + self.settings.sync_timer_delta,
        )

    @lazy_wrapper(BatchRequestPayload)
    def receive_batch_request(self, p: Peer, request: BatchRequestPayload):
        """Receive batch request"""
        missing_batches = [b_obj.batch_id for b_obj in request.missing if b_obj.batch_id in self.batches]
        for batch_id in missing_batches:
            self.ez_send(p, self.batches[batch_id])

    def run(self):
        if self.is_transaction_creator:
            self.start_tx_creation()
        else:
            self.start_batch_making()
            self.start_header_making()
            self.start_sync_timer()
