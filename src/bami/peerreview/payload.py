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
    format_list = ["I", "?", "74s", "I", "64s", "64s"]
    names = ["sn", "is_send", "cp_pk", "cp_sn", "m_hash", "p_hash"]


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
