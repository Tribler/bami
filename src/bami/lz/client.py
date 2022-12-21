import dataclasses
import random
from abc import abstractmethod, ABC

from bami.lz.base import BaseMixin
from bami.lz.database import PeerTxDB
from bami.lz.payload import TransactionPayload
from bami.lz.utils import payload_hash
from bami.peerreview.utils import get_random_string


class TransactionProducer(BaseMixin, ABC):

    def create_transaction(self):
        script = get_random_string(self.settings.script_size)
        new_tx = TransactionPayload(script.encode())
        tx_hash = payload_hash(new_tx)

        self.peer_db.add_peer_tx(self.my_peer_id, tx_hash)
        self.peer_db.add_tx_payload(tx_hash, new_tx)

    def start_tx_creation(self):
        self.register_task(
            "create_transaction",
            self.create_transaction,
            interval=random.random() + self.settings.tx_freq,
            delay=random.random() + self.settings.tx_delay,
        )
