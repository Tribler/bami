from ipv8.types import ConfigBuilder

from bami.spar.community import SPARCommunity
from simulation import BamiSimulation, SimulatedCommunityMixin


class AbstractBlockchainSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SPARCommunity", "my peer", [], [], {"settings": self.settings.overlay_settings}, [])
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


class BlockchainSPARCommunity(SPARCommunity, SimulatedCommunityMixin):

    """Community to exchange transactions and blocks on the network overlay for a
    Bitcoin-like blockchain.
    1. Received transactions from some client. For the simulation we generate transaction at each peer randomly.
    2. Create a block. Share the block with the neighbors.
    3. Exchange blocks between each other. Syncronizing the blockchain.
    When peer receives a message (transaction or block) it will remember the sender of the message.
    Later the message is evaluated against the usefullness of the message. If the message is usefull, the sender is
    rewarded.
    """

    @lazy_wrapper(TransactionPayload)
    def received_transaction(self, tx: Transaction):
        super().received_transaction(tx)
        self.log("Received transaction %s from %s" % (tx.tx_id, tx.sender_id))


