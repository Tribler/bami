from ipv8.messaging.payload import Payload


class BlockPayload(Payload):
    """
    Payload for message that ships a signed block
    """

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

    def __init__(
        self,
        block_type,
        transaction,
        public_key,
        sequence_number,
        previous,
        links,
        com_id,
        com_seq_num,
        signature,
        timestamp,
    ):
        super(BlockPayload, self).__init__()
        self.type = block_type
        self.transaction = transaction
        self.public_key = public_key
        self.sequence_number = sequence_number
        self.previous = previous
        self.links = links
        self.com_id = com_id
        self.com_seq_num = com_seq_num
        self.signature = signature
        self.timestamp = timestamp

    @classmethod
    def from_block(cls, block):
        return BlockPayload(
            block.type,
            block._transaction,
            block.public_key,
            block.sequence_number,
            block._previous,
            block._links,
            block.com_id,
            block.com_seq_num,
            block.signature,
            block.timestamp,
        )

    def to_pack_list(self):
        data = [
            ("varlenI", self.type),
            ("varlenI", self.transaction),
            ("74s", self.public_key),
            ("I", self.sequence_number),
            ("varlenI", self.previous),
            ("varlenI", self.links),
            ("74s", self.com_id),
            ("I", self.com_seq_num),
            ("64s", self.signature),
            ("Q", self.timestamp),
        ]

        return data

    @classmethod
    def from_unpack_list(cls, *args):
        return BlockPayload(*args)


class BlockBroadcastPayload(BlockPayload):
    """
    Payload for a message that contains a half block and a TTL field for broadcasts.
    """

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
        "I",
    ]

    def __init__(
        self,
        block_type,
        transaction,
        public_key,
        sequence_number,
        previous,
        links,
        com_id,
        com_seq_num,
        signature,
        timestamp,
        ttl,
    ):
        super(BlockBroadcastPayload, self).__init__(
            block_type,
            transaction,
            public_key,
            sequence_number,
            previous,
            links,
            com_id,
            com_seq_num,
            signature,
            timestamp,
        )
        self.ttl = ttl

    @classmethod
    def from_block_gossip(cls, block, ttl):
        return BlockBroadcastPayload(
            block.type,
            block._transaction,
            block.public_key,
            block.sequence_number,
            block._previous,
            block._links,
            block.com_id,
            block.com_seq_num,
            block.signature,
            block.timestamp,
            ttl,
        )

    def to_pack_list(self):
        data = super(BlockBroadcastPayload, self).to_pack_list()
        data.append(("I", self.ttl))
        return data

    @classmethod
    def from_unpack_list(cls, *args):
        return BlockBroadcastPayload(*args)


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


class SubscriptionsPayload(KVPayload):
    pass


class FrontierPayload(KVPayload):
    pass


class BlocksRequestPayload(KVPayload):
    pass


class BlockResponsePayload(KVPayload):
    pass


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
