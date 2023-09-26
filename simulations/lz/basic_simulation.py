from asyncio import ensure_future, get_event_loop
from typing import Iterable

from ipv8.configuration import ConfigBuilder

from bami.lz.community import SyncCommunity
from bami.lz.payload import TransactionPayload
from bami.lz.settings import LZSettings, SettlementStrategy
from common.utils import random_topology, time_mark
from simulations.settings import DefaultLocations, LocalLocations, SimulationSettings
from simulations.simulation import BamiSimulation, SimulatedCommunityMixin


class BasicLZSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("LZCommunity", "my peer", [], [], {"settings": self.settings.overlay_settings}, [])
        return builder

    def on_discovery_start(self):
        super().on_discovery_start()

        TX_FILE = self.settings.consts.get('TX_FILE')
        SETTLE_FILE = self.settings.consts.get('SETTLE_FILE')

        with open(TX_FILE, "w") as out:
            out.write("peer_id,tx_id,time\n")
        with open(SETTLE_FILE, "w") as out:
            out.write("peer_id,tx_id,time\n")

        client_to_ignore = None

        for i, peer_id in enumerate(self.nodes.keys()):
            self.nodes[peer_id].overlays[0].TX_FILE = TX_FILE
            self.nodes[peer_id].overlays[0].SETTLE_FILE = SETTLE_FILE

            if i < self.settings.clients:
                self.nodes[peer_id].overlays[0].make_light_client()
                if i == 0:
                    client_to_ignore = self.nodes[peer_id].overlays[0].my_peer_id
            if self.settings.clients < i < self.settings.clients + self.settings.faulty:
                self.nodes[peer_id].overlays[0].censor_peer(client_to_ignore)

    def on_discovery_complete(self):
        super().on_discovery_complete()
        for i, peer_id in enumerate(self.nodes.keys()):
            if self.nodes[peer_id].overlays[0].is_light_client:
                self.nodes[peer_id].overlays[0].start_tx_creation()
            else:
                self.nodes[peer_id].overlays[0].start_reconciliation()
                self.nodes[peer_id].overlays[0].start_periodic_settlement()
                self.nodes[peer_id].overlays[0].start_batch_making()


class SimulateLZCommunity(SimulatedCommunityMixin, SyncCommunity):
    on_received_reconciliation_request = time_mark(SyncCommunity.on_received_reconciliation_request)
    on_received_transactions_request = time_mark(SyncCommunity.on_received_transactions_request)
    on_received_transactions_challenge = time_mark(SyncCommunity.on_received_transactions_challenge)
    on_received_reconciliation_response = time_mark(SyncCommunity.on_received_reconciliation_response)
    reconcile_with_neighbors = time_mark(SyncCommunity.reconcile_with_neighbors)
    on_received_transaction_batch = time_mark(SyncCommunity.on_received_transaction_batch)
    on_received_transaction = time_mark(SyncCommunity.on_received_transaction)
    settle_transactions = time_mark(SyncCommunity.settle_transactions)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def on_process_new_transaction(self, t_id: int, tx_payload: TransactionPayload):
        # Write to the database - transaction added, time
        with open(self.TX_FILE, "a") as out:
            out.write("{},{},{}\n".format(hash(self.my_peer), t_id, get_event_loop().time()))
        super().on_process_new_transaction(t_id, tx_payload)

    def on_settle_transactions(self, settled_txs: Iterable[int]):
        super().on_settle_transactions(settled_txs)
        with open(self.SETTLE_FILE, "a") as out:
            for t_id in settled_txs:
                out.write("{},{},{}\n".format(hash(self.my_peer), t_id, get_event_loop().time()))


def main(prefix="", sim_settings: SimulationSettings = None):
    if sim_settings:
        settings = sim_settings
    else:
        LATENCY = "global"
        N_CLIENTS = 10
        N_PEERS = 190
        N_FAULTS = 0
        N = N_CLIENTS + N_PEERS

        settings = SimulationSettings()
        settings.clients = N_CLIENTS
        settings.peers = N
        settings.faulty = N_FAULTS
        settings.duration = 30
        d = 8
        settings.topology = random_topology(N, d)
        settings.logging_level = 'WARNING'
        settings.discovery_delay = 5
        settings.location_latency_generator = LocalLocations if LATENCY == 'local' else DefaultLocations

        settings.community_map = {'LZCommunity': SimulateLZCommunity}

        class SimSettings(LZSettings):
            recon_freq = 2
            recon_fanout = 8
            tx_batch = 1
            tx_freq = 1 / 10
            initial_fanout = 5

            sketch_size = 100
            settle_size = 350
            settle_freq = 1
            settle_delay = 1
            settle_strategy = SettlementStrategy.VANILLA

        settings.overlay_settings = SimSettings()

        DIR_PREFIX = "../../lz_visualize/" + prefix

        DATA_FILE = DIR_PREFIX + "_n_{}_t_{}_f_{}_d_{}_t_{:.1f}_s_{}".format(N,
                                                                             d,
                                                                             SimSettings.recon_fanout,
                                                                             SimSettings.recon_freq,
                                                                             SimSettings.tx_batch / SimSettings.tx_freq * N_CLIENTS,
                                                                             SimSettings.settle_freq
                                                                             )

        TX_FILE = DATA_FILE + ".csv"
        SD_FILE = DATA_FILE + "_data.csv"
        SETTLE_FILE = DATA_FILE + "_set.csv"

        settings.consts = {'TX_FILE': TX_FILE, "SD_FILE": SD_FILE, "SETTLE_FILE": SETTLE_FILE}

    simulation = BasicLZSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    for peer_id in simulation.nodes.keys():
        print(peer_id, len(simulation.nodes[peer_id].overlays[0].memcache.tx_payloads))

    for peer_id in simulation.nodes.keys():
        print(peer_id,
              simulation.nodes[peer_id].overlays[0].receive_counter,
              simulation.nodes[peer_id].overlays[0].send_counter,
              )

    SD_FILE = settings.consts.get("SD_FILE")

    with open(SD_FILE, "w") as out_data:
        for peer_id in simulation.nodes.keys():
            for i in range(len(simulation.nodes[peer_id].overlays[0].sketch_stat_has)):
                out_data.write("{},{},{}\n".format(
                    peer_id,
                    simulation.nodes[peer_id].overlays[0].sketch_stat_has[i],
                    simulation.nodes[peer_id].overlays[0].sketch_stat_miss[i]
                ))


if __name__ == "__main__":
    import sys

    # Access the input value
    # input_value = int(sys.argv[1])
    input_value = 1
    prefix = ""
    main("net_data/" + str(input_value) + prefix)
