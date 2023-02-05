from binascii import hexlify
import string
import random
from hashlib import sha256
import sys

from ipv8.messaging.serialization import default_serializer


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def payload_hash(tx_payload, serializer=default_serializer):
    pack = serializer.pack_serializable(tx_payload)
    return sha256(pack).digest()


def bytes_to_uint(val: bytes, size: int = 32) -> int:
    return int(hexlify(val), 16) % 2 ** size


def uint_to_bytes(val: int, size: int = 32) -> bytes:
    return val.to_bytes(size // 8, 'big', signed=False)


def uhash(val):
    return hash(val) % ((sys.maxsize + 1) * 2)
