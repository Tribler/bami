from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from ipv8.messaging.serialization import Payload


@vp_compile
class PullPayload(VariablePayload):
    """
    Request the view of a target peer.
    """

    msg_id = 1


@vp_compile
class PeerPayload(VariablePayload):
    """
    Payload that contains info on a single peer.
    """

    format_list = ["ipv4", "74s"]
    names = ["address", "public_key"]


class PushPayload(Payload):
    """
    Nested payload that contains multiple PeerPayload.
    """

    msg_id = 2
    format_list = [[PeerPayload]]

    def __init__(self, peers):
        self.peers = peers

    def to_pack_list(self):
        return [("payload-list", self.peers)]

    @classmethod
    def from_unpack_list(cls, *args):
        return PushPayload(*args)
