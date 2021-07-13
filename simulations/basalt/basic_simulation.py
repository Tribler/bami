from asyncio import ensure_future

from ipv8.configuration import ConfigBuilder

from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation


class BasicBasaltSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("BasaltCommunity", "my peer", [], [], {}, [])
        return builder


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 20
    settings.duration = 20
    simulation = BasicBasaltSimulation(settings)
    ensure_future(simulation.run())

    simulation.loop.run_forever()
