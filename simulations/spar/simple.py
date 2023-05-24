from asyncio import ensure_future

import networkx as nx
from ipv8.types import ConfigBuilder

from bami.spar.community import SPARCommunity
from bami.spar.settings import SPARSettings
from common.utils import connected_topology
from settings import SimulationSettings, LocalLocations, DefaultLocations
from simulation import BamiSimulation, SimulatedCommunityMixin


class DummySPAR(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SPARCommunity", "my peer",
                            [], [], {"settings": self.settings.overlay_settings}, [])
        return builder

    def on_discovery_start(self):
        super().on_discovery_start()

    def on_discovery_complete(self):
        super().on_discovery_complete()

        peer_map = {}
        for peer_id in self.nodes.keys():
            peer_map[self.nodes[peer_id].overlays[0].my_peer_id] = peer_id

        for i, peer_id in enumerate(self.nodes.keys()):
            self.nodes[peer_id].overlays[0].run()
            self.nodes[peer_id].overlays[0].peer_map = peer_map

            if peer_id < self.settings.faulty:
                self.nodes[peer_id].overlays[0].share_ratio = 0.2


class SimulateSPARCommunity(SimulatedCommunityMixin, SPARCommunity):
    pass


def main(sim_settings: SimulationSettings = None):
    if sim_settings:
        settings = sim_settings
    else:
        LATENCY = "global"
        N = 20

        settings = SimulationSettings()
        settings.peers = N
        settings.faulty = 5
        settings.duration = 100
        settings.topology = connected_topology(N)
        print("Real global graph",
              settings.topology.number_of_nodes(),
              settings.topology.number_of_edges())
        settings.logging_level = 'INFO'
        settings.discovery_delay = 3
        settings.location_latency_generator = LocalLocations if LATENCY == 'local' else DefaultLocations

        settings.community_map = {'SPARCommunity': SimulateSPARCommunity}

        class SimSettings(SPARSettings):
            pass

        settings.overlay_settings = SimSettings()

        DIR_PREFIX = "../../lz_visualize/data/spar"

        DATA_FILE = DIR_PREFIX + "_n_{}_{}".format(N, LATENCY)
        TX_FILE = DATA_FILE + ".csv"
        SD_FILE = DATA_FILE + "_data.csv"
        SETTLE_FILE = DATA_FILE + "_set.csv"
        settings.consts = {'TX_FILE': TX_FILE, "SD_FILE": SD_FILE, "SETTLE_FILE": SETTLE_FILE}

    simulation = DummySPAR(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    # A mapping peer_id to peer_pk

    peer_map = {}
    for peer_id in simulation.nodes.keys():
        peer_map[simulation.nodes[peer_id].overlays[0].my_peer_id] = peer_id

    PREFIX = "../../spar_visual/"
    for peer_id in simulation.nodes.keys():
        G = simulation.nodes[peer_id].overlays[0].rank.graph.copy()
        # Map node ids to peer ids
        mapping = {v: peer_map[v] for v in G.nodes()}
        G = nx.relabel_nodes(G, mapping)
        nx.write_weighted_edgelist(G,
                                   PREFIX+"graph_{}.csv".format(peer_id))


    with open("../../spar_visual/spar_ranks.csv", "a+") as f:
        for peer_id in simulation.nodes.keys():
            peer_pk = simulation.nodes[peer_id].overlays[0].my_peer_id

            ranks = simulation.nodes[peer_id].overlays[0].rank.get_ranks(peer_pk)
            # Write rankings of a peer to a file
            for v, r in ranks.items():
                f.write("{},{},{}\n".format(peer_id, peer_map[v], r))


if __name__ == "__main__":
    main()
