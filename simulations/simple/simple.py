from asyncio import ensure_future, get_event_loop, set_event_loop, sleep
import os

from ipv8.community import Community
from ipv8.configuration import ConfigBuilder
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from ipv8_service import IPv8

from common.discrete_loop import DiscreteLoop
from common.network import SimulatedNetwork
from common.simulation_endpoint import SimulationEndpoint
from settings import DefaultLocations


@vp_compile
class PingMessage(VariablePayload):
    msg_id = 1


@vp_compile
class PongMesage(VariablePayload):
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
        self.ez_send(peer, PongMesage())

    @lazy_wrapper(PongMesage)
    def on_pong_message(self, peer, payload):
        self.logger.info("🧊 <t=%.1f> peer %s received pong", get_event_loop().time(), self.my_peer.address)


async def start_communities():
    instances = []
    network = SimulatedNetwork(DefaultLocations)
    for i in range(1, 10):
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "medium", f"ec{i}.pem")
        # builder.add_overlay("PingPongCommunity", "my peer", [], [], {}, [('started',)])
        builder.add_overlay("BasaltCommunity", "my peer", [], [], {}, [('started',)])
        endpoint = SimulationEndpoint(network)
        instance = IPv8(builder.finalize(), endpoint_override=endpoint,
                        extra_communities={'BasaltCommunity': PingPongCommunity})
        await instance.start()
        instances.append(instance)

    # Introduce peers to each other
    for from_instance in instances:
        for to_instance in instances:
            if from_instance == to_instance:
                continue
            from_instance.overlays[0].walk_to(to_instance.endpoint.wan_address)


async def run_simulation():
    await start_communities()
    await sleep(10)
    get_event_loop().stop()

if __name__ == "__main__":
    # We use a discrete event loop to enable quick simulations.
    loop = DiscreteLoop()
    set_event_loop(loop)
    ensure_future(run_simulation())
    loop.run_forever()