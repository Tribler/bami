from dataclasses import dataclass


@dataclass
class SimulationSettings:
    peers: int = 100  # Number of IPv8 peers
    duration: int = 120  # Simulation duration in sections
    profile: bool = False  # Whether to run the Yappi profiler
