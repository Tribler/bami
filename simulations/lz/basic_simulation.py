from asyncio import ensure_future

from ipv8.configuration import ConfigBuilder

from bami.lz.community import SyncCommunity
from common.utils import connected_topology
from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation


class BasicLZSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("LZCommunity", "my peer", [], [], {}, [])
        return builder

    def on_discovery_complete(self):
        super().on_discovery_complete()

        self.nodes[1].overlays[0].start_tx_creation()
        for peer_id in self.nodes.keys():
            self.nodes[peer_id].overlays[0].start_reconciliation()


if __name__ == "__main__":
    settings = SimulationSettings()
    N = 20
    settings.peers = N
    settings.duration = 40
    settings.topology = connected_topology(N)
    settings.logging_level = 'DEBUG'
    settings.discovery_delay = 5

    settings.community_map = {'LZCommunity': SyncCommunity}

    simulation = BasicLZSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    ni2key = {}
    for ni, node in simulation.nodes.items():
        ni2key[node.overlays[0].my_peer_id] = ni

    def pprint(peer_txs): return [(ni2key[k], len(vals)) for k, vals in peer_txs.items()]

    for ni, node in simulation.nodes.items():
        print("Known transactions", list(node.overlays[0].db.tx_payloads.keys()))
