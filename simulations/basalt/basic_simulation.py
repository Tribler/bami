from asyncio import ensure_future
import logging

from ipv8.configuration import ConfigBuilder

from bami.basalt.community import BasaltCommunity
from common.utils import connected_topology
from simulations.settings import LocalLocations, SimulationSettings
from simulations.simulation import BamiSimulation


class BasicBasaltSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("BasaltCommunity", "my peer", [], [], {}, [])
        return builder


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 50
    settings.duration = 20
    settings.topology = connected_topology(50)


    '''
    logging.basicConfig(format='%(message)s',
                        filename='logs.log',
                        filemode='w',
                        level=logging.INFO)
    '''

    simulation = BasicBasaltSimulation(settings, {'BasaltCommunity': BasaltCommunity})
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    key_to_id = {node_ins.overlays[0].my_peer.mid: node_num for node_num, node_ins in simulation.nodes.items()}
    print([key_to_id[p.mid] for p in simulation.nodes[1].overlays[0].view])
