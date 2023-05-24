from ipv8.messaging.lazy_payload import VariablePayload, vp_compile
from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@vp_compile
class CompactClock(VariablePayload):
    format_list = ['Q', 'varlenH']
    names = ['add', 'data']


@dataclass(msg_id=1)
class WorkCertificatePayload:
    pk: bytes
    o_pk: bytes
    score: int
    seq_num: int
    sign: bytes
    clock: int


@dataclass(msg_id=2)
class CertificateRequestPayload:
    topic: bytes
    last_clock: int


@dataclass(msg_id=3)
class CertificateBatchPayload:
    batch: [WorkCertificatePayload]


@dataclass(msg_id=4)
class ConnectionRequestPayload:
    pass


@dataclass(msg_id=5)
class ConnectionRejectPayload:
    pass


@dataclass(msg_id=6)
class UsefulBlobPayload:
    blob_id: bytes
    fee: int


@dataclass(msg_id=7)
class BlobBatchPayload:
    batch: [UsefulBlobPayload]


@dataclass
class CompactBloomFilter:
    data: bytes
    seed: int
    csum: bytes
