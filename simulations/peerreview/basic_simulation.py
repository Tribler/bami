from asyncio import ensure_future

from ipv8.configuration import ConfigBuilder

from bami.peerreview.community import PeerReviewCommunity
from common.utils import connected_topology, time_mark
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
    received_logged_message = time_mark(PeerReviewCommunity.received_logged_message)


if __name__ == "__main__":
    settings = SimulationSettings()
    N = 50
    settings.peers = N
    settings.duration = 10
    settings.topology = connected_topology(N)
    settings.logging_level = 'INFO'
    settings.discovery_delay = 5

    settings.community_map = {'PeerReviewCommunity': PeerReviewCommunity}

    simulation = BasicPeerReviewSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    ni2key = {}
    for ni, node in simulation.nodes.items():
        ni2key[node.overlays[0].my_peer_id] = ni

    def pprint(peer_txs): return [(ni2key[k], len(vals)) for k, vals in peer_txs.items()]

    for ni, node in simulation.nodes.items():
        print("Node {} knows about {}".format(ni, pprint(node.overlays[0].known_peer_txs.peer_txs)))
