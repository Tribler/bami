from asyncio import ensure_future, get_event_loop
from typing import Iterable

from ipv8.configuration import ConfigBuilder

from bami.broadcast.community import MempoolBroadcastCommunity
from bami.broadcast.payload import HeaderPayload, TxBatchPayload
from bami.broadcast.payload import TransactionPayload
from bami.broadcast.settings import MempoolBroadcastSettings
from common.utils import random_topology, time_mark, connected_topology
from simulations.settings import DefaultLocations, LocalLocations, SimulationSettings
from simulations.simulation import BamiSimulation, SimulatedCommunityMixin


class NarwhalBroadcastSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("NarwhalBroadcast", "my peer", [],
                            [], {"settings": self.settings.overlay_settings}, [])
        return builder

    def on_discovery_start(self):
        super().on_discovery_start()

        for i, peer_id in enumerate(self.nodes.keys()):
            if i < self.settings.clients:
                self.nodes[peer_id].overlays[0].is_transaction_creator = True

    def on_discovery_complete(self):
        super().on_discovery_complete()
        for i, peer_id in enumerate(self.nodes.keys()):
            self.nodes[peer_id].overlays[0].my_peer_num = peer_id
            self.nodes[peer_id].overlays[0].run()


class SimulatedBroadcastCommunity(SimulatedCommunityMixin, MempoolBroadcastCommunity):
    feed_batch_maker = time_mark(MempoolBroadcastCommunity.feed_batch_maker)
    broadcast = time_mark(MempoolBroadcastCommunity.broadcast)
    lucky_broadcast = time_mark(MempoolBroadcastCommunity.lucky_broadcast)
    receive_new_batch = time_mark(MempoolBroadcastCommunity.receive_new_batch)
    receive_batch_ack = time_mark(MempoolBroadcastCommunity.receive_batch_ack)
    receive_header = time_mark(MempoolBroadcastCommunity.receive_header)
    receive_batch_request = time_mark(MempoolBroadcastCommunity.receive_batch_request)

    def __init__(self, *args, **kwargs) -> None:
        self.my_peer_num = - 1
        super().__init__(*args, **kwargs)

        self.tx_start_time = {}
        self.tx_ready_time = {}

    # noinspection PyUnreachableCode
    def on_transaction_created(self, new_tx: TransactionPayload):
        # Write time when we've first seen the transaction
        self.tx_start_time[new_tx.tx_id] = get_event_loop().time()
        super().on_transaction_created(new_tx)

    def on_transaction_finalized(self, tx: TransactionPayload):
        # Write time when transaction is finalized
        self.tx_ready_time[tx.tx_id] = get_event_loop().time()
        super().on_transaction_finalized(tx)


def main(prefix="", sim_settings: SimulationSettings = None):
    if sim_settings:
        settings = sim_settings
    else:
        LATENCY = "global"
        N_CLIENTS = 10
        N = 200

        settings = SimulationSettings()
        settings.clients = N_CLIENTS
        settings.peers = N
        settings.duration = 20

        d = 8
        settings.topology = connected_topology(N)
        settings.logging_level = 'WARNING'
        settings.discovery_delay = 5
        settings.location_latency_generator = LocalLocations if LATENCY == 'local' else DefaultLocations

        settings.community_map = {'NarwhalBroadcast': SimulatedBroadcastCommunity}
        settings.overlay_settings = MempoolBroadcastSettings()

    simulation = NarwhalBroadcastSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    for peer_id in simulation.nodes.keys():
        print(peer_id,
              simulation.nodes[peer_id].overlays[0].receive_counter,
              simulation.nodes[peer_id].overlays[0].send_counter,
              )

    # Collect transaction start time, merge dictionaries
    total_tx_start_times = {}
    for peer_id in simulation.nodes.keys():
        total_tx_start_times.update(simulation.nodes[peer_id].overlays[0].tx_start_time)

    # Collect transaction ready time, merge dictionaries
    total_tx_ready_times = {}
    for peer_id in simulation.nodes.keys():
        for tx_id, ready_time in simulation.nodes[peer_id].overlays[0].tx_ready_time.items():
            # Start time
            latency = ready_time - total_tx_start_times[tx_id]
            if tx_id not in total_tx_ready_times:
                total_tx_ready_times[tx_id] = []
            total_tx_ready_times[tx_id].append(latency)
    # Report for 10 random transaction min, avg, max
    import random
    random_tx_ids = random.sample(list(total_tx_ready_times.keys()), min(10, len(total_tx_ready_times.keys())))
    for tx_id in random_tx_ids:
        print(len(total_tx_ready_times[tx_id]),
              min(total_tx_ready_times[tx_id]),
              sum(total_tx_ready_times[tx_id]) / len(total_tx_ready_times[tx_id]),
              max(total_tx_ready_times[tx_id]))


if __name__ == "__main__":
    import sys

    # Access the input value
    # input_value = int(sys.argv[1])
    input_value = 1
    prefix = ""
    main()
