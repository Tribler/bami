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

    def on_new_batch_created(self, new_batch: TxBatchPayload):
        super().on_new_batch_created(new_batch)
        print("Peer {}: New batch with {} # txs created at time {}".format(self.my_peer_num,
                                                                           len(new_batch.txs),
                                                                           get_event_loop().time()))

    # noinspection PyUnreachableCode
    def on_transaction_created(self, new_tx: TransactionPayload):
        # Write time when we've first seen the transaction
        return None
        print("Peer {}: New transaction {} at time {}".format(self.my_peer_num,
                                                              new_tx.tx_id,
                                                              get_event_loop().time()))

    def on_new_header(self, new_header: HeaderPayload):
        super().on_new_header(new_header)
        # Write the time when transaction is finalized
        for batch_ack in new_header.batches:
            batch = self.batches[batch_ack.batch_id]
            print("Peer {}: Number of transactions finalized {} at time {}".format(
                self.my_peer_num,
                len(batch.txs),
                get_event_loop().time()))


def main(prefix="", sim_settings: SimulationSettings = None):
    if sim_settings:
        settings = sim_settings
    else:
        LATENCY = "global"
        N_CLIENTS = 10
        N = 40

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
        print(peer_id, len(simulation.nodes[peer_id].overlays[0].batches))


if __name__ == "__main__":
    import sys

    # Access the input value
    # input_value = int(sys.argv[1])
    input_value = 1
    prefix = ""
    main("net_data/" + str(input_value) + prefix)
