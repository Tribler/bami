from dataclasses import dataclass
from typing import Optional, Dict, Type

import networkx as nx
from ipv8.types import Community

from simulation.common.utils import random_topology
from simulation.common.config import Config, Dist


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


@dataclass
class SimulationSettings:
    # Number of IPv8 peers.
    peers: int = 100

    # The name of the experiment.
    name: str = ""

    # Whether to run the Yappi profiler.
    profile: bool = False

    # An optional identifier for the experiment, appended to the working directory name.
    identifier: Optional[str] = None

    # The duration of the simulation in seconds.
    duration: int = 120

    # The duration for discovery
    discovery_delay: int = 5

    # The logging level during the experiment.
    logging_level: str = "INFO"

    # Whether we enable statistics like message sizes and frequencies.
    enable_community_statistics: bool = False

    # Config class for latency specification, as latency distribution matrix
    location_latency_generator: Config = LocalLocations

    # Optional topology for the overlay network
    topology: Optional[nx.DiGraph] = None

    # The IPv8 ticker is responsible for community walking and discovering other peers, but can significantly limit
    # performance. Setting this option to False cancels the IPv8 ticker, improving performance.
    enable_ipv8_ticker: bool = True

    # A map for community_name: community class implementation to be used in simulation
    community_map: Optional[Dict[str, Type[Community]]] = None

    def __post_init__(self):
        if not self.topology:
            self.topology = random_topology(self.peers)
