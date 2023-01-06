from bami.peerreview.payload import TxsChallengePayload, TxId
from bami.peerreview.utils import payload_hash


def test_nested_payload():
    rq1 = TxsChallengePayload([TxId(b'test1'), TxId(b'test2')])
    assert payload_hash(rq1) is not None
