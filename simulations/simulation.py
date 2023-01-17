import asyncio
import logging
import os
import shutil
import time
from asyncio import sleep, get_event_loop
from typing import Optional

import yappi
from ipv8.configuration import ConfigBuilder
from ipv8.messaging.interfaces.statistics_endpoint import StatisticsEndpoint
from ipv8.types import Peer, AnyPayload
from ipv8_service import IPv8

from common.discrete_loop import DiscreteLoop
from common.network import SimulatedNetwork
from common.simulation_endpoint import SimulationEndpoint
from common.utils import set_time_mark
from simulations.settings import SimulationSettings


class SimulatedCommunityMixin:
    """Mixin to replace the message send with a delay from previous marker.
    This also requires to assign _start_time in the start of critical zone using `set_time_mark`, or `time_mark`.
    """

    async def busy_wait(self):
        end_time = time.perf_counter()
        delta = end_time - self._start_time
        await asyncio.sleep(delta)

    def end_point(self, task, *args):
        end_time = time.perf_counter()
        delta = end_time - self._start_time
        get_event_loop().call_later(delta, task, *args)

    def start_point(self):
        set_time_mark(self)


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
    MAIN_OVERLAY: Optional[str] = None

    def __init__(self, settings: SimulationSettings) -> None:
        super().__init__()
        self.settings = settings
        self.nodes = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        dir_name = "n_%d" % self.settings.peers if not self.settings.identifier else \
            "n_%d_%s" % (self.settings.peers, self.settings.identifier)
        if self.settings.name:
            dir_name = "%s_%s" % (dir_name, self.settings.name)
        self.data_dir = os.path.join("data", dir_name)
        self.address_to_location = {}
        self.network = SimulatedNetwork(settings.location_latency_generator)

        if settings.community_map:
            self.communities = settings.community_map
        else:
            self.communities = {}

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
            endpoint = SimulationEndpoint(self.network)
            config = self.get_ipv8_builder(peer_id)
            config.set_log_level(self.settings.logging_level)
            instance = IPv8(config.finalize(), endpoint_override=endpoint,
                            extra_communities=self.communities)
            if self.settings.enable_community_statistics:
                instance.endpoint = StatisticsEndpoint(endpoint)

            await instance.start()

            if not self.settings.enable_ipv8_ticker:
                # Disable the IPv8 ticker
                instance.state_machine_task.cancel()

            # Set the WAN address of the peer to the address of the endpoint
            for overlay in instance.overlays:
                overlay.max_peers = -1
                overlay.my_peer.address = instance.overlays[0].endpoint.wan_address
                overlay.my_estimated_wan = instance.overlays[0].endpoint.wan_address

            # If we have a main overlay set, find it and assign it to the overlay attribute
            instance.overlay = None
            if self.MAIN_OVERLAY:
                for overlay in instance.overlays:
                    if overlay.__class__.__name__ == self.MAIN_OVERLAY:
                        instance.overlay = overlay
                        break

            if self.settings.enable_community_statistics:
                for overlay in instance.overlays:
                    overlay.endpoint = instance.endpoint
                    instance.endpoint.enable_community_statistics(overlay.get_prefix(), True)

            self.nodes[peer_id] = instance

    def setup_directories(self) -> None:
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

    def check_connected(self) -> bool:
        for peer_id in self.nodes.keys():
            neigh_set = list(self.settings.topology.neighbors(peer_id))
            if len(self.nodes[peer_id].overlays[0].get_peers()) < len(neigh_set):
                return False
        return True

    def on_discovery_start(self):
        pass

    async def ipv8_discover_peers(self) -> None:
        self.on_discovery_start()
        for peer_id in self.nodes.keys():
            neigh_set = self.settings.topology.neighbors(peer_id)
            for node_b_id in list(neigh_set):
                self.nodes[peer_id].overlays[0].walk_to(self.nodes[node_b_id].endpoint.wan_address)
        await sleep(self.settings.discovery_delay)
        print("IPv8 peer discovery complete")
        self.on_discovery_complete()

    def on_discovery_complete(self):
        pass

    async def start_simulation(self) -> None:
        print("Starting simulation with %d peers..." % self.settings.peers)

        if self.settings.profile:
            yappi.start(builtins=True)

        start_time = time.perf_counter()
        await asyncio.sleep(self.settings.duration)
        print("Simulation took %f seconds" % (time.perf_counter() - start_time))

        if self.settings.profile:
            yappi.stop()
            yappi_stats = yappi.get_func_stats()
            yappi_stats.sort("tsub")
            yappi_stats.save(os.path.join(self.data_dir, "yappi.stats"), type='callgrind')

        self.loop.stop()

    async def on_ipv8_ready(self) -> None:
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
        start_time = time.perf_counter()
        await self.start_ipv8_nodes()
        await self.ipv8_discover_peers()
        await self.on_ipv8_ready()
        print("Simulation setup took %f seconds" % (time.perf_counter() - start_time))
        await self.start_simulation()
        self.on_simulation_finished()
