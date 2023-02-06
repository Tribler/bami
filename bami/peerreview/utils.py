import string
import random
from hashlib import sha256

from ipv8.messaging.serialization import default_serializer


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def payload_hash(tx_payload, serializer=default_serializer):
    pack = serializer.pack_serializable(tx_payload)
    return sha256(pack).digest()
