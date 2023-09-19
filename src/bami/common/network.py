from typing import Optional, Type

import networkx as nx

from bami.common.config import Config, Dist
from bami.common.utils import Cache


class DefaultLocations(Config):
    locations = Dist('sample', {'weight': [0.3, 0.3, 0.4], 'values': ['Tokyo', 'Ireland', 'Ohio']})
    latencies = {
        'Ohio': {'Ohio': Dist('invgamma', (10.54090, 0.333305, 0.987249)),
                 'Ireland': Dist('norm', (73.6995, 1.19583092197097127)),
                 'Tokyo': Dist('norm', (156.00904977375566, 0.09469886668079797))
                 },
        'Ireland': {'Ireland': Dist('invgamma', (12.4360455224301525, 0.8312748033308526, 1.086191852963273)),
                    'Tokyo': Dist('norm', (131.0275, 0.25834811785650774))
                    },
        'Tokyo': {'Tokyo': Dist('invgamma', (20.104508341331055, 0.3371934865734555, 2.0258998705983737))}
    }


class LocalLocations(Config):
    locations = Dist('sample', ['local'])
    latencies = {'local': {'local': 1}}


class SimulatedNetwork:

    def __init__(self,
                 location_latencies_generator: Type[Config] = DefaultLocations,
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

    def fix_location(self, peer_id: str):
        self.adr_location[peer_id] = self.locations.fetch()

    def get_link_latency(self, src, dest) -> float:
        """
        Return the link latency from source to a particular destination address, in seconds.
        """
        loc1 = self.adr_location[src]
        loc2 = self.adr_location[dest]
        # Latencies are in specified in ms
        return self.latencies.fetch(loc1, loc2) / 1000


