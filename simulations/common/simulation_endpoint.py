from asyncio import get_event_loop

from ipv8.test.mocking.endpoint import AutoMockEndpoint
from ipv8.util import succeed

from common.network import SimulatedNetwork


class SimulationEndpoint(AutoMockEndpoint):
    """
    Endpoint used in a simulated IPv8 environment. We make the open function async since this is expected by
    the IPv8 service
    """

    def __init__(self, network: SimulatedNetwork):
        super().__init__()

        self.network = network

        loc = network.locations.fetch()
        network.adr_location[self.wan_address] = loc
        network.adr_location[self.lan_address] = loc

    async def open(self):
        self._open = True
        return succeed(None)

    def send(self, socket_address, packet):
        get_event_loop().call_later(
            self.network.get_link_latency(self.get_address(), socket_address),
            super().send,
            socket_address, packet)
