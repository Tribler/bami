import os
from asyncio import ensure_future, get_event_loop

from ipv8.community import Community
from ipv8.configuration import ConfigBuilder
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.lazy_payload import VariablePayload, vp_compile

from simulation.common.settings import SimulationSettings
from simulation.common.simulation import SimulatedCommunityMixin, BamiSimulation
from simulation.common.utils import time_mark, connected_topology


@vp_compile
class PingMessage(VariablePayload):
    msg_id = 1


@vp_compile
class PongMessage(VariablePayload):
    msg_id = 2


class PingPongCommunity(Community):
    """
    This basic community sends ping messages to other known peers every two seconds.
    """
    community_id = os.urandom(20)

    def __init__(self, my_peer, endpoint, network):
        super().__init__(my_peer, endpoint, network)
        self.add_message_handler(1, self.on_ping_message)
        self.add_message_handler(2, self.on_pong_message)

    def started(self):
        self.register_task("send_ping", self.send_ping, interval=2.0, delay=0)

    def send_ping(self):
        self.logger.info("🔥 <t=%.1f> peer %s sending ping", get_event_loop().time(), self.my_peer.address)
        for peer in self.network.verified_peers:
            self.ez_send(peer, PingMessage())

    @lazy_wrapper(PingMessage)
    def on_ping_message(self, peer, payload):
        self.logger.info("🔥 <t=%.1f> peer %s received ping", get_event_loop().time(), self.my_peer.address)
        self.logger.info("🧊 <t=%.1f> peer %s sending pong", get_event_loop().time(), self.my_peer.address)
        self.ez_send(peer, PongMessage())

    @lazy_wrapper(PongMessage)
    def on_pong_message(self, peer, payload):
        self.logger.info("🧊 <t=%.1f> peer %s received pong", get_event_loop().time(), self.my_peer.address)


class BasicPingPongSimulation(BamiSimulation):
    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("PingPongCommunity", "my peer", [], [], {}, [('started',)])
        return builder


class SimulatedPingPong(SimulatedCommunityMixin, PingPongCommunity):
    send_ping = time_mark(PingPongCommunity.send_ping)
    on_ping_message = time_mark(PingPongCommunity.on_ping_message)


if __name__ == "__main__":
    # We use a discrete event loop to enable quick simulations.
    settings = SimulationSettings()
    settings.peers = 6
    settings.duration = 1000
    settings.topology = connected_topology(settings.peers)
    settings.community_map = {'PingPongCommunity': SimulatedPingPong}

    simulation = BasicPingPongSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()
