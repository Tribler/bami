from asyncio import ensure_future

from ipv8.configuration import ConfigBuilder

from bami.basalt.community import BasaltCommunity
from common.utils import connected_topology
from simulations.settings import SimulationSettings
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
    settings.community_map = {'BasaltCommunity': BasaltCommunity}

    simulation = BasicBasaltSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()
