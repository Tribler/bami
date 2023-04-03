from collections import defaultdict
import random

from ipv8.community import DEFAULT_MAX_PEERS
from ipv8.lazy_community import lazy_wrapper_unsigned
from ipv8.peer import Peer
import networkx as nx

from bami.spar.base import BaseCommunity
from bami.spar.payload import CertificateBatchPayload, CertificateRequestPayload, TransactionPayload, \
    WorkCertificatePayload
from bami.spar.random_walks import BiasStrategies, RandomWalks
from bami.spar.rank import IncrementalPageRank
from bami.spar.score import LocalCounterScore
from bami.spar.settings import SPARSettings
from bami.spar.sync_clock import ClockTable, SyncClock


class SPARCommunity(BaseCommunity):
    """
    The community to enable decentralized network collaboration.
    It has four main functionalities:
    - Score: to score peers based on the amount of work they have done.
    - Populate: to request data of score graph from other peers using crawler.
    - Analyze: rank all peers based on the received and estimated scores.
    - Readjust: to adjust the network based on the consistent ranking of the peers. Such as: Starvation of Freeriders.
    """

    def __init__(self, my_peer, endpoint, network, max_peers=DEFAULT_MAX_PEERS, anonymize=False, **kwargs):
        self.settings = kwargs.pop("settings", SPARSettings())
        super().__init__(my_peer, endpoint, network, max_peers, anonymize)

        self.local_scores = defaultdict(lambda: 0)
        self.work_certificates = defaultdict(dict)

        self.last_sync_clocks = defaultdict(lambda: ClockTable())
        self.last_seq_num = defaultdict(lambda: 0)

        self.work_graph = nx.DiGraph()
        self.rank = IncrementalPageRank()

    @property
    def my_peer_id(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def update_score(self, peer_id: bytes, delta: int) -> None:
        self.local_scores[peer_id] += delta

    def get_score(self, peer_id: bytes) -> int:
        return self.local_scores[peer_id]

    def create_certificate(self, peer_id: bytes) -> None:
        """
        Create a certificate to score the peer
        """
        val = self.score_module.get_score(peer_id)
        seq_num = self.last_seq_num[peer_id] + 1

        wcp = WorkCertificatePayload(self.my_peer_id,
                                     peer_id, val,
                                     seq_num, b'')
        sign = self.crypto.create_signature(self.my_peer.key,
                                            self.prepare_packet(wcp, sig=False))
        wcp.sign = sign
        self.work_certificates[self.my_peer_id][peer_id] = wcp
        self.last_sync_clocks[self.my_peer_id].increment(peer_id)

    def update_work_graph(self, payload: WorkCertificatePayload) -> None:
        """
        Update the work graph with the received certificate
        """
        last_known_cert = self.work_certificates[payload.pk].get(payload.o_pk)
        if last_known_cert is None:
            self.rank.add_edge(payload.pk, payload.o_pk, weight=payload.score)
            self.work_certificates[payload.pk][payload.o_pk] = payload
        else:
            if payload.seq_num > last_known_cert.seq_num:
                self.rank.add_edge(payload.pk, payload.o_pk, weight=payload.score)
                self.work_certificates[payload.pk][payload.o_pk] = payload

    def crawl_step(self) -> None:
        """
        Perform a single step of the crawler.
        """
        # Run unweighted random walk
        v = RandomWalks(self.work_graph) \
            .run_one_walk(self.my_peer.public_key.key_to_bin(),
                          reset_probability=0.2,
                          bias_strategy=BiasStrategies.NO_WEIGHT,
                          )
        topic = v[-1]
        known_peers = {p.public_key.key_to_bin() for p in self.get_peers()}
        inter = set(v) & known_peers
        if len(inter) > 0:
            neigh = random.choice(list(inter))
        else:
            neigh = random.choice(list(self.get_peers()))
            topic = neigh.public_key.key_to_bin()
        last_clock = self.last_sync_clocks[topic]
        # Request any update from the neigh
        self.ez_send(neigh,
                     CertificateRequestPayload(topic,
                                               last_clock.compact_clock())
                     )

    def run(self) -> None:
        # Start the crawler
        self.register_task("crawl", self.crawl_step, interval=self.settings.crawl_interval)

    @lazy_wrapper_unsigned(CertificateRequestPayload)
    def received_certificate_request(self, p: Peer, payload: CertificateRequestPayload):
        """
        Receive a certificate request from a peer and respond with selection of certificates.
        """
        # Update the last sync clock
        peer_clock = SyncClock.from_compact_clock(payload.last_clock)

        new_batch = []
        connected = self.last_sync_clocks[payload.topic].sorted_diff(peer_clock)

        for p in connected:
            new_batch.append(self.work_certificates[payload.topic][p])
            if len(new_batch) >= self.settings.batch_size:
                break
        cert_batch = CertificateBatchPayload(new_batch)
        self.ez_send(p, cert_batch)

    @lazy_wrapper_unsigned(CertificateBatchPayload)
    def received_certificate_batch(self, p: Peer, payload: CertificateBatchPayload):
        """
        Receive a certificate batch from a peer and update the work graph.
        """
        for cert in payload.batch:
            self.update_work_graph(cert)
