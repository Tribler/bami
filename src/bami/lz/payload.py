from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass

from bami.lz.sketch.peer_clock import CompactClock

dataclass = overwrite_dataclass(dataclass)


@dataclass(msg_id=1)
class TransactionPayload:
    pk: bytes
    t_id: bytes
    sign: bytes
    script: bytes
    context: bytes


@dataclass
class CompactSketch:
    data: bytes
    seed: int
    csum: bytes


@dataclass
class SketchData:
    d: bytes
    csum: bytes


@dataclass
class CompositeSketch:
    seed: int
    d: [SketchData]


@dataclass(msg_id=2)
class ReconciliationRequestPayload:
    clock: CompactClock
    sketch: CompactSketch


@vp_compile
class TransactionsChallengePayload(VariablePayload):
    format_list = ["varlenH-list"]
    names = ["txs"]
    msg_id = 3


@dataclass(msg_id=4)
class ReconciliationResponsePayload:
    clock: CompactClock
    sketch: CompactSketch
    txs: TransactionsChallengePayload


@vp_compile
class TransactionsRequestPayload(VariablePayload):
    format_list = ["varlenH-list"]
    names = ["txs"]
    msg_id = 5


@dataclass(msg_id=6)
class TransactionBatchPayload:
    txs: [TransactionPayload]
