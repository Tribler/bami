import asyncio
import os
import random
import shutil
import time

import yappi

from bami.basalt.community import BasaltCommunity
from ipv8.configuration import ConfigBuilder
from ipv8_service import IPv8

from simulation.discrete_loop import DiscreteLoop
from simulation.simulation_endpoint import SimulationEndpoint

from simulations.settings import SimulationSettings


class BamiSimulation:
    """
    The main logic to run simulations with the various algorithms included in BAMI.
    To create your own simulation, you should subclass the BamiSimulation class and override the get_ipv8_builder
    method to load custom communities. One can override on_simulation_finished to parse data after the simulation
    is finished.

    One should pass a SimulationSettings object when initializing this class. This object contains various settings
    related to the simulation, for example, the number of peers.

    Each experiment will write data to a subdirectory in the data directory. The name of this subdirectory depends
    on the simulation settings.
    """

    def __init__(self, settings: SimulationSettings) -> None:
        self.settings = settings
        self.nodes = []
        self.data_dir = os.path.join("data", "n_%d" % self.settings.peers)

        self.loop = DiscreteLoop()
        asyncio.set_event_loop(self.loop)

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", os.path.join(self.data_dir, f"ec{peer_id}.pem"))
        return builder

    async def start_ipv8_nodes(self) -> None:
        for peer_id in range(1, self.settings.peers + 1):
            if peer_id % 100 == 0:
                print("Created %d peers..." % peer_id)
            endpoint = SimulationEndpoint()
            instance = IPv8(self.get_ipv8_builder(peer_id).finalize(), endpoint_override=endpoint,
                            extra_communities={'BasaltCommunity': BasaltCommunity})
            await instance.start()
            self.nodes.append(instance)

    def setup_directories(self) -> None:
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

    def ipv8_discover_peers(self) -> None:
        for node_a in self.nodes:
            connect_nodes = random.sample(self.nodes, min(100, len(self.nodes)))
            for node_b in connect_nodes:
                if node_a == node_b:
                    continue

                node_a.overlays[0].walk_to(node_b.endpoint.wan_address)
        print("IPv8 peer discovery complete")

    async def start_simulation(self) -> None:
        print("Starting simulation with %d peers..." % self.settings.peers)

        if self.settings.profile:
            yappi.start(builtins=True)

        start_time = time.time()
        await asyncio.sleep(self.settings.duration)
        print("Simulation took %f seconds" % (time.time() - start_time))

        if self.settings.profile:
            yappi.stop()
            yappi_stats = yappi.get_func_stats()
            yappi_stats.sort("tsub")
            yappi_stats.save(os.path.join(self.data_dir, "yappi.stats"), type='callgrind')

        self.loop.stop()

    def on_ipv8_ready(self) -> None:
        """
        This method is called when IPv8 is started and peer discovery is finished.
        """
        pass

    def on_simulation_finished(self) -> None:
        """
        This method is called when the simulations are finished.
        """
        pass

    async def run(self) -> None:
        self.setup_directories()
        await self.start_ipv8_nodes()
        self.ipv8_discover_peers()
        self.on_ipv8_ready()
        await self.start_simulation()
        self.on_simulation_finished()
