from asyncio import ensure_future
import logging

from ipv8.configuration import ConfigBuilder

from bami.peerreview.community import PeerReviewCommunity
from common.utils import connected_topology, set_time_mark, time_mark
from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation, SimulatedCommunityMixin


class BasicPeerReviewSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("PeerReviewCommunity", "my peer", [], [], {}, [])
        return builder


class SimulatedBasalt(SimulatedCommunityMixin, PeerReviewCommunity):
    received_txs_proof = time_mark(PeerReviewCommunity.received_txs_proof)
    received_tx_request = time_mark(PeerReviewCommunity.received_tx_request)
    received_txs_challenge = time_mark(PeerReviewCommunity.received_txs_challenge)
    reconcile_with_neighbors = time_mark(PeerReviewCommunity.reconcile_with_neighbors)
    random_push = time_mark(PeerReviewCommunity.random_push)


if __name__ == "__main__":
    settings = SimulationSettings()
    N = 100
    settings.peers = N
    settings.duration = 120
    settings.topology = connected_topology(N)
    settings.logging_level = 'WARNING'

    print("E=", settings.topology.number_of_edges())
    settings.community_map = {'PeerReviewCommunity': PeerReviewCommunity}

    simulation = BasicPeerReviewSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

