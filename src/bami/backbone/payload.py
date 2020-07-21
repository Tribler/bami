from ipv8.messaging.lazy_payload import VariablePayload, vp_compile


class ComparablePayload(VariablePayload):
    def __eq__(self, o: VariablePayload) -> bool:
        return self.format_list == o.format_list and self.names == o.names


@vp_compile
class RawBlockPayload(ComparablePayload):
    msg_id = 1
    format_list = ["varlenH"]
    names = ["block_bytes"]


@vp_compile
class BlockPayload(ComparablePayload):
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
class RawBlockBroadcastPayload(ComparablePayload):
    msg_id = 3
    format_list = ["varlenH", "I"]
    names = ["block_bytes", "ttl"]


@vp_compile
class BlockBroadcastPayload(BlockPayload):
    """
    Payload for a message that contains a half block and a TTL field for broadcasts.
    """

    msg_id = 4
    format_list = BlockPayload.format_list + ["I"]
    names = BlockPayload.names + ["ttl"]


@vp_compile
class FrontierPayload(ComparablePayload):
    msg_id = 5
    format_list = ["varlenH", "varlenH"]
    names = ["chain_id", "frontier"]


@vp_compile
class ExtendedFrontierPayload(ComparablePayload):
    msg_id = 6
    format_list = ["varlenH", "varlenH", "74s", "64s", "varlenH"]
    names = ["chain_id", "frontier", "pub_key", "signature", "state_blob"]


@vp_compile
class SubscriptionsPayload(ComparablePayload):
    msg_id = 7
    format_list = ["74s", "varlenH"]
    names = ["public_key", "subcoms"]


@vp_compile
class BlocksRequestPayload(ComparablePayload):
    msg_id = 8
    format_list = ["74s", "varlenH"]
    names = ["subcom_id", "frontier_diff"]


class StateRequestPayload(ComparablePayload):
    msg_id = 9
    format_list = ["74s", "varlenH"]
    names = ["chain_id", "state_request"]


class StateResponsePayload(ComparablePayload):
    msg_id = 10
    format_list = ["74s", "varlenH"]
    names = ["subcom_id", "frontier_diff"]
