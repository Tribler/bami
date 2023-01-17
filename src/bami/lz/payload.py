from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@vp_compile
class CompactClock(VariablePayload):
    format_list = ['Q', 'varlenH']
    names = ['add', 'data']


@dataclass(msg_id=1)
class TransactionPayload:
    pk: bytes
    t_id: bytes
    sign: bytes
    script: bytes
    context: bytes


@dataclass
class CompactBloomFilter:
    data: bytes
    seed: int
    csum: bytes


@vp_compile
class CompactMiniSketch(VariablePayload):
    format_list = ["H", "H", "varlenH"]
    names = ['o', 't', 'data']


@dataclass(msg_id=2)
class ReconciliationRequestPayload:
    clock: CompactClock
    sk_type: str
    sketch: bytes


@vp_compile
class TransactionsChallengePayload(VariablePayload):
    format_list = ["varlenH-list"]
    names = ["txs"]
    msg_id = 3


@dataclass(msg_id=4)
class ReconciliationResponsePayload:
    clock: CompactClock
    sk_type: str
    sketch: bytes
    txs: TransactionsChallengePayload


@vp_compile
class TransactionsRequestPayload(VariablePayload):
    format_list = ["varlenH-list"]
    names = ["txs"]
    msg_id = 5


@dataclass(msg_id=6)
class TransactionBatchPayload:
    txs: [TransactionPayload]
