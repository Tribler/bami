from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@vp_compile
class TransactionPayload(VariablePayload):
    """
    Raw transaction blob
    """
    msg_id = 1
    format_list = ["varlenH"]
    names = ["script"]


@vp_compile
class LogEntryPayload(VariablePayload):
    """
    Log entry in tamper-evident log of a peer
    """
    msg_id = 2
    format_list = ["74s", "I", "?", "64s", "74s", "I", "varlenH"]
    names = ["pk", "sn", "is_send", "p_hash", "cp_pk", "cp_sn", "msg"]


@dataclass
class TxId:
    tx_id: bytes


@dataclass(msg_id=3)
class TxsChallengePayload:
    tx_ids: [TxId]


@dataclass(msg_id=4)
class TxsRequestPayload:
    tx_ids: [TxId]


@dataclass(msg_id=5)
class TxsProofPayload:
    tx_ids: [TxId]


@dataclass(msg_id=6)
class LoggedMessagePayload:
    """Log entry with number sn in a tamper evident log of peer with public key pk."""
    pk: bytes
    sn: int
    p_hash: bytes
    sign: bytes
    msg: TransactionPayload
    lh: bytes


@vp_compile
class LoggedAuthPayload(VariablePayload):
    msg_id = 7
    format_list = ["74s", "I", "64s", "64s"]
    names = ["pk", "sn", "lh", "sign"]
