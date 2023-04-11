from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@dataclass()
class TxID:
    t: bytes
    f: int


@dataclass(msg_id=5)
class BlockPayload:
    block_id: bytes
    tx_list: [TxID]
    seq_num: int
    prev_block: bytes
    creator: bytes


@dataclass(msg_id=6)
class BlockRequestPayload:
    block_id: bytes
