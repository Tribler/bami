from typing import Optional, Type

import networkx as nx

from common.config import Config
from common.utils import Cache


class SimulatedNetwork:

    def __init__(self,
                 location_latencies_generator: Type[Config],
                 topology: Optional[nx.Graph] = None
                 ):
        """ Simulated IPv8 network.
        Message is directly passed to other peer.
        @type all_nodes: map between peer_id and peer's ipv8 instance.
        """
        self.adr_location = {}
        self.locations = Cache(location_latencies_generator.locations)
        self.latencies = Cache(location_latencies_generator.latencies)
        self.topology = topology

    def get_link_latency(self, src, dest) -> float:
        """
        Return the link latency from source to a particular destination address, in seconds.
        """
        loc1 = self.adr_location[src]
        loc2 = self.adr_location[dest]
        return self.latencies.fetch(loc1, loc2)
