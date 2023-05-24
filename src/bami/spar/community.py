import csv
import os
import random
from binascii import unhexlify
from collections import defaultdict

from ipv8.community import DEFAULT_MAX_PEERS
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer

from bami.spar.base import BaseCommunity
from bami.spar.payload import CertificateBatchPayload, CertificateRequestPayload, \
    WorkCertificatePayload, ConnectionRequestPayload, ConnectionRejectPayload, UsefulBlobPayload, BlobBatchPayload
from bami.spar.rank import IncrementalMeritRank
from bami.spar.settings import SPARSettings

from collections import deque

from bami.spar.tx_generator import FeeGenerator


class LSL:
    def __init__(self, limit):
        self.limit = limit
        self.items = deque(maxlen=limit)

    def append(self, item):
        self.items.append(item)

    def __getitem__(self, index):
        return self.items[index]

    def __len__(self):
        return len(self.items)


class SPARCommunity(BaseCommunity):
    """
    The community to enable decentralized network collaboration.
    It has four main functionalities:
    - Score: to score peers based on the amount of work they have done.
    - Populate: to request data of score graph from other peers using crawler.
    - Analyze: rank all peers based on the received and estimated scores.
    - Readjust: to adjust the network based on the consistent ranking of the peers. Such as: Starvation of Freeriders.
    """

    community_id = unhexlify("6c6564676572207a65726f206973206772656004")

    def __init__(self, my_peer, endpoint, network, max_peers=DEFAULT_MAX_PEERS, anonymize=False, **kwargs):
        self.settings = kwargs.pop("settings", SPARSettings())
        super().__init__(my_peer, endpoint, network, max_peers, anonymize)

        self.local_scores = defaultdict(lambda: 0)
        self.work_certificates = defaultdict(dict)

        self.last_clocks = defaultdict(lambda: 0)
        self.last_seq_num = defaultdict(lambda: 0)

        self.epoch = 0

        self.current_neighbors = []

        self.my_clock = 0
        self.bootstrap_period = 2
        self.gamma = 0

        self.prop = 1
        self.blobs = LSL(self.settings.max_blobs)
        self.fee = FeeGenerator()

        self.share_ratio = 1.0
        self.add_sybils = False
        self.sybil_count = self.settings.sybil_count

        self.rank = IncrementalMeritRank(alpha=0.2)
        self.first_update = True

        self.add_message_handler(CertificateRequestPayload, self.received_certificate_request)
        self.add_message_handler(CertificateBatchPayload, self.received_certificate_batch)
        self.add_message_handler(ConnectionRequestPayload, self.received_connection_request)
        self.add_message_handler(ConnectionRejectPayload, self.received_connection_reject)
        self.add_message_handler(WorkCertificatePayload, self.received_work_certificate)
        self.add_message_handler(BlobBatchPayload, self.received_blob_batch)

    def get_peer_by_key(self, key: bytes) -> Peer:
        if b"sybil" in key:
            key = key.split(b"sybil")[0]
        for peer in self.get_peers():
            if peer.public_key.key_to_bin() == key:
                return peer
        return None

    def get_peer_keys(self) -> [bytes]:
        return [peer.public_key.key_to_bin() for peer in self.get_peers()]

    def select_new_peers(self) -> [Peer]:
        """
        Select peers based on the current ranking
        """
        self.current_neighbors = self.get_peers()
        return
        self.bootstrap_period -= 1
        if self.bootstrap_period <= 0:
            self.gamma = self.settings.target_gamma
        n = int(self.gamma * self.settings.min_slots)
        if n > 0 and self.rank.graph.has_node(self.my_peer_id):
            ranks = self.rank.get_ranks(self.my_peer_id)
            reputable_peers = list(ranks.keys())

            s_peers = random.choices(reputable_peers,
                                     weights=list(ranks.values()),
                                     k=n * 2)
            repu_peers = list(set(s_peers))[:n]
        else:
            repu_peers = []

        amount = self.settings.min_slots - len(repu_peers)
        others = random.sample(list(set(self.get_peer_keys()) - set(repu_peers)), amount)

        # Send to selected peers connection request
        self.current_neighbors = [self.get_peer_by_key(key) for key in repu_peers + others]
        for peer in self.current_neighbors:
            self.ez_send(peer, ConnectionRequestPayload())

    @lazy_wrapper(ConnectionRequestPayload)
    def received_connection_request(self, peer, _):
        """
        Received connection request from a peer
        """
        if len(self.current_neighbors) >= self.settings.max_slots:
            self.ez_send(peer, ConnectionRejectPayload())
            return
        self.current_neighbors.append(peer)

    @lazy_wrapper(ConnectionRejectPayload)
    def received_connection_reject(self, peer, _):
        """
        Received connection reject from a peer
        """
        self.current_neighbors.remove(peer)

    @property
    def my_peer_id(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def create_certificate(self, peer_id: bytes, value: int) -> None:
        """
        Create a certificate to score the peer
        """
        self.last_seq_num[peer_id] += 1
        self.my_clock += 1
        wcp = WorkCertificatePayload(self.my_peer_id,
                                     peer_id, value,
                                     self.last_seq_num[peer_id], b'', self.my_clock)
        sign = self.crypto.create_signature(self.my_peer.key,
                                            self.prepare_packet(wcp, sig=False))
        wcp.sign = sign
        self.update_work_graph(wcp)

        return wcp

    def update_work_graph(self, payload: WorkCertificatePayload) -> None:
        """
        Update the work graph with the received certificate
        """
        last_known_cert = self.work_certificates[payload.pk].get(payload.o_pk)
        if last_known_cert is None:
            self.rank.graph.add_edge(payload.pk, payload.o_pk, weight=payload.score)
            self.work_certificates[payload.pk][payload.o_pk] = payload
        else:
            if payload.seq_num > last_known_cert.seq_num:
                self.rank.graph.add_edge(payload.pk, payload.o_pk, weight=payload.score)
                self.work_certificates[payload.pk][payload.o_pk] = payload

    def crawl_step(self) -> None:
        """
        Perform a single step of the crawler.
        """
        # Run unweighted random walk
        neigh = random.choice(self.current_neighbors)
        topic = neigh.public_key.key_to_bin()
        self.ez_send(neigh,
                     CertificateRequestPayload(topic, 0),
                     )

    def dummy_blob_create(self) -> None:
        """
        Dummy score update to test the network
        """
        if random.random() <= self.prop:
            # generate random blob bytes of size 10
            blob_id = os.urandom(10)
            blob_fee = self.fee.generate_fee()
            if random.random() <= self.share_ratio:
                self.blobs.append(UsefulBlobPayload(blob_id, blob_fee))

        selected = random.choices(self.current_neighbors, k=4)

        if self.share_ratio < 1.0:
            blobs = random.sample(list(self.blobs),
                                  k=min(len(self.blobs), int(self.share_ratio * len(self.blobs))))
        else:
            blobs = self.blobs

        if random.random() <= self.share_ratio:
            for p in selected:
                self.ez_send(p, BlobBatchPayload(list(blobs)))

    @lazy_wrapper(BlobBatchPayload)
    def received_blob_batch(self, sender_peer: Peer, payload: BlobBatchPayload):
        """
        Receive a batch of blobs
        """
        for blob in payload.batch:
            # if blob not in self.blobs:
            if blob not in self.blobs:
                self.blobs.append(blob)
                p_id = sender_peer.public_key.key_to_bin()
                last_score = self.local_scores[p_id] + 1
                wcp = self.create_certificate(p_id,
                                              last_score)
                self.ez_send(sender_peer, wcp)
                self.local_scores[p_id] = last_score

    @lazy_wrapper(WorkCertificatePayload)
    def received_work_certificate(self, sender_peer: Peer, payload: WorkCertificatePayload):
        """
        Receive a batch of certificates
        """
        self.update_work_graph(payload)

    def recalc_rank(self) -> None:
        if not self.rank.graph.has_node(self.my_peer_id):
            return
        self.rank.calculate(self.my_peer_id, num_walks=2000)

        # write to the file currently selected peers
        data = [(self.peer_map[self.my_peer_id],
                 self.peer_map[peer.public_key.key_to_bin()],
                 self.epoch) for peer in self.current_neighbors]

        with open("../../spar_visual/selected_peers.csv", "a+") as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row)
        self.epoch += 1

    def run(self) -> None:
        # Start the crawler
        self.register_task("crawl", self.crawl_step,
                           interval=self.settings.crawl_interval,
                           delay=random.random() + self.settings.dummy_score_interval)
        self.register_task("rank", self.recalc_rank, delay=self.settings.rank_recal_delay,
                           interval=random.random() + self.settings.rank_recal_interval)
        self.select_new_peers()

        #self.register_task("select", self.select_new_peers,
        #                   interval=self.settings.peer_shuffle_interval,
        #                   delay=random.random())
        if self.settings.dummy_score_interval > 0:
            self.register_task("dummy_create", self.dummy_blob_create,
                               interval=random.random() + self.settings.dummy_score_interval)

    def create_sybil_certificate(self):
        for i in range(self.sybil_count):
            # generate random blob bytes of size 10
            blob_id = os.urandom(10)
            blob_fee = self.fee.generate_fee()
            self.blobs.append(UsefulBlobPayload(blob_id, blob_fee))

    @lazy_wrapper(CertificateRequestPayload)
    def received_certificate_request(self, sender_peer: Peer, payload: CertificateRequestPayload):
        """
        Receive a certificate request from a peer and respond with selection of certificates.
        """
        # Update the last sync clock
        last_clock = payload.last_clock

        all_values = [value for inner_dict in self.work_certificates.values()
                      for value in inner_dict.values()]
        values_sample = random.sample(all_values, k=min(len(all_values), 10))
        new_batch = [k for k in values_sample]
        cert_batch = CertificateBatchPayload(new_batch)
        self.ez_send(sender_peer, cert_batch)

    @lazy_wrapper(CertificateBatchPayload)
    def received_certificate_batch(self, p: Peer, payload: CertificateBatchPayload):
        """
        Receive a certificate batch from a peer and update the work graph.
        """
        for cert in payload.batch:
            self.update_work_graph(cert)
