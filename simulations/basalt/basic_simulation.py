from asyncio import ensure_future

from ipv8.configuration import ConfigBuilder

from bami.basalt.community import BasaltCommunity
from common.utils import random_topology, set_time_mark, time_mark
from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation, SimulatedCommunityMixin


class BasicBasaltSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("BasaltCommunity", "my peer", [], [], {}, [])
        return builder


class SimulatedBasalt(SimulatedCommunityMixin, BasaltCommunity):
    received_pull = time_mark(BasaltCommunity.received_pull)

    def peer_update(self) -> None:
        set_time_mark(self)
        peer = self.select_peer()
        self.send_pull(peer)
        set_time_mark(self)
        peer = self.select_peer()
        self.send_push(peer)


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 25
    settings.duration = 60
    settings.topology = random_topology(25)
    settings.community_map = {'BasaltCommunity': SimulatedBasalt}

    simulation = BasicBasaltSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()
