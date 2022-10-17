from ipv8.messaging.lazy_payload import VariablePayload, vp_compile


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


@vp_compile
class PushPayload(VariablePayload):
    """
    Nested payload that contains multiple PeerPayload.
    """
    msg_id = 2
    format_list = [[PeerPayload]]
    names = ["peers"]
