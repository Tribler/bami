from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from ipv8.messaging.payload import Payload


@vp_compile
class RawBlockPayload(VariablePayload):
    msg_id = 1
    format_list = ["varlenH"]
    names = ["block_bytes"]


@vp_compile
class BlockPayload(VariablePayload):
    msg_id = 2
    format_list = [
        "varlenI",
        "varlenI",
        "74s",
        "I",
        "varlenI",
        "varlenI",
        "74s",
        "I",
        "64s",
        "Q",
    ]
    names = [
        "type",
        "transaction",
        "public_key",
        "sequence_number",
        "previous",
        "links",
        "com_id",
        "com_seq_num",
        "signature",
        "timestamp",
    ]


@vp_compile
class BlockBroadcastPayload(BlockPayload):
    """
    Payload for a message that contains a half block and a TTL field for broadcasts.
    """

    msg_id = 3
    format_list = BlockPayload.format_list + ["I"]
    names = BlockPayload.names + ["ttl"]


@vp_compile
class FrontierPayload(VariablePayload):
    msg_id = 4
    format_list = ["varlenH", "varlenH"]
    names = ["chain_id", "frontier"]


@vp_compile
class ExtendedFrontierPayload(VariablePayload):
    msg_id = 5
    format_list = ["varlenH", "varlenH", "74s", "64s", "varlenH"]
    names = ["chain_id", "frontier", "pub_key", "signature", "state_blob"]


@vp_compile
class SubscriptionsPayload(VariablePayload):
    msg_id = 6
    format_list = ["74s", "varlenH"]
    names = ["public_key", "subcoms"]


@vp_compile
class BlocksRequestPayload(VariablePayload):
    msg_id = 7
    format_list = ["74s", "varlenH"]
    names = ["subcom_id", "frontier_diff"]


class KVPayload(Payload):
    format_list = ["varlenI", "varlenI"]

    def __init__(self, key, value):
        Payload.__init__(self)
        self.key = key
        self.value = value

    def to_pack_list(self):
        return [("varlenI", self.key), ("varlenI", self.value)]

    @classmethod
    def from_unpack_list(cls, key, value):
        return KVPayload(key, value)


class StateRequestPayload(KVPayload):
    pass


class StateResponsePayload(KVPayload):
    pass


class StateByHashRequestPayload(KVPayload):
    pass


class StateByHashResponsePayload(KVPayload):
    pass


class AuditRequestPayload(KVPayload):
    pass


class AuditProofPayload(KVPayload):
    pass


class AuditProofRequestPayload(KVPayload):
    """
    Payload that holds a request for an audit proof.
    """

    pass


class AuditProofResponsePayload(KVPayload):
    """
    Payload that holds the response with an audit proof or chain state.
    """

    pass


class PingPayload(Payload):
    format_list = ["I"]

    def __init__(self, identifier):
        super(PingPayload, self).__init__()
        self.identifier = identifier

    def to_pack_list(self):
        return [("I", self.identifier)]

    @classmethod
    def from_unpack_list(cls, identifier):
        return PingPayload(identifier)
