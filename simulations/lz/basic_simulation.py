from asyncio import ensure_future, get_event_loop

from ipv8.configuration import ConfigBuilder

from bami.lz.community import SyncCommunity
from bami.lz.payload import TransactionPayload
from bami.lz.settings import LZSettings
from common.utils import connected_topology, random_topology, time_mark
from simulations.settings import DefaultLocations, LocalLocations, SimulationSettings
from simulations.simulation import BamiSimulation, SimulatedCommunityMixin


class SimSettings(LZSettings):
    recon_freq = 1
    recon_fanout = 3
    tx_batch = 3
    tx_freq = 0.1
    initial_fanout = 1

    sketch_size = 100


LATENCY = "global"
N_CLIENTS = 20
N_PEERS = 10
N = N_CLIENTS + N_PEERS


class BasicLZSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("LZCommunity", "my peer", [], [], {"settings": SimSettings()}, [])
        return builder

    def on_discovery_start(self):
        super().on_discovery_start()
        for i, peer_id in enumerate(self.nodes.keys()):
            if i < N_CLIENTS:
                self.nodes[peer_id].overlays[0].make_light_client()

    def on_discovery_complete(self):
        super().on_discovery_complete()
        for i, peer_id in enumerate(self.nodes.keys()):
            if self.nodes[peer_id].overlays[0].is_light_client:
                self.nodes[peer_id].overlays[0].start_tx_creation()
            else:
                self.nodes[peer_id].overlays[0].start_reconciliation()


DATA_FILE = "../../lz_visualize/data/tx_time_mem_n_{}_{}_d_{}_t_{:.2f}".format(N, LATENCY,
                                                                               SimSettings.recon_freq,
                                                                               SimSettings.tx_batch / SimSettings.tx_freq * N_CLIENTS
                                                                               )
TX_FILE = DATA_FILE + ".csv"
STAT_FILE = DATA_FILE + "_rounds.csv"
SD_FILE = DATA_FILE + "_data.csv"


class SimulateLZCommunity(SimulatedCommunityMixin, SyncCommunity):
    on_received_reconciliation_request = time_mark(SyncCommunity.on_received_reconciliation_request)
    on_received_transactions_request = time_mark(SyncCommunity.on_received_transactions_request)
    on_received_transactions_challenge = time_mark(SyncCommunity.on_received_transactions_challenge)
    on_received_reconciliation_response = time_mark(SyncCommunity.on_received_reconciliation_response)
    reconcile_with_neighbors = time_mark(SyncCommunity.reconcile_with_neighbors)
    on_received_transaction_batch = time_mark(SyncCommunity.on_received_transaction_batch)

    def __init__(self, *args, **kwargs) -> None:
        with open(TX_FILE, "w") as out:
            out.write("peer_id,tx_id,time\n")
        super().__init__(*args, **kwargs)

    def on_process_new_transaction(self, t_id: int, tx_payload: TransactionPayload):
        # Write to the database - transaction added, time
        with open(TX_FILE, "a") as out:
            out.write("{},{},{}\n".format(hash(self.my_peer), t_id, get_event_loop().time()))
        super().on_process_new_transaction(t_id, tx_payload)


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = N
    settings.duration = 60
    settings.topology = connected_topology(N)
    settings.logging_level = 'INFO'
    settings.discovery_delay = 5
    settings.location_latency_generator = LocalLocations if LATENCY == 'local' else DefaultLocations

    settings.community_map = {'LZCommunity': SimulateLZCommunity}

    simulation = BasicLZSimulation(settings)
    ensure_future(simulation.run())
    simulation.loop.run_forever()

    for peer_id in simulation.nodes.keys():
        print(peer_id, len(simulation.nodes[peer_id].overlays[0].memcache.tx_payloads))

    with open(SD_FILE, "w") as out_data:
        for peer_id in simulation.nodes.keys():
            for i in range(len(simulation.nodes[peer_id].overlays[0].sketch_stat_has)):
                out_data.write("{},{},{}\n".format(
                    peer_id,
                    simulation.nodes[peer_id].overlays[0].sketch_stat_has[i],
                    simulation.nodes[peer_id].overlays[0].sketch_stat_miss[i]
                ))
