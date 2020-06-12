from ast import literal_eval
from binascii import hexlify
from hashlib import sha256
from typing import Set, Tuple, Any, Dict, NewType

KEY_LEN = 8
ShortKey = NewType("ShortKey", str)
BytesLinks = NewType("BytesLinks", bytes)
Links = NewType("Links", Tuple[Tuple[int, ShortKey]])
Ranges = NewType("Ranges", Tuple[Tuple[int, int]])


def shorten(key) -> ShortKey:
    return ShortKey(hexlify(key)[-KEY_LEN:].decode())


def hex_to_int(hex_val) -> int:
    return int(hexlify(hex_val), 16) % 100000000


def short_to_int(short_key: ShortKey) -> int:
    return int(short_key, 16)


def int_to_short_key(int_val: int) -> ShortKey:
    val = hex(int_val)[2:]
    while len(val) < KEY_LEN:
        val = "0" + val
    return ShortKey(val)


def decode_frontier(frontier: dict):
    """
    Decode for packet
    """
    decoded = dict()
    for k, v in frontier.items():
        if k in ("h", "m", "state"):
            decoded[k] = v
        else:
            decoded[k] = decode_links(v)
    return decoded


# Key -> Value
def encode_frontier(frontier: Dict[Any, Any]) -> Dict[Any, Any]:
    """Encode to python dict

    Args:
        frontier:
    """
    encoded = dict()
    for k, v in frontier.items():
        if k in ("h", "m", "state"):
            encoded[k] = v
        else:
            encoded[k] = encode_links(v)
    return encoded


def take_hash(value: Any) -> bytes:
    """Encode to bytes and return hash digest

    Args:
        value: python object with __repr__

    Returns:
        Hash in bytes
    """
    return sha256(encode_raw(value)).digest()


def encode_raw(val: Any) -> bytes:
    """Encode python object with __repr__ to bytes"""
    return repr(val).encode()


def decode_raw(byte_raw: bytes) -> Any:
    """Decode bytes to python struct"""
    return literal_eval(byte_raw.decode())


def encode_links(link_val: Links) -> BytesLinks:
    """Encode to the sendable packet
    Args:
        link_val:

    Returns:
        links encoded in bytes
    """
    return BytesLinks(encode_raw(link_val))


def decode_links(bytes_links: BytesLinks) -> Links:
    """Decode bytes to links

    Args:
        bytes_links: bytes containing links value

    Returns:
        Links values
    """
    return Links(decode_raw(bytes_links))


def expand_ranges(range_vals: Ranges) -> Set[int]:
    """Expand ranges to set of ints

    Args:
        range_vals: List of tuple ranges

    Returns:
        Set of ints with ranges expanded
    """
    val_set = set()
    for b, e in range_vals:
        for val in range(b, e + 1):
            val_set.add(val)
    return val_set


def ranges(nums: Set[int]) -> Ranges:
    """Compress numbers to tuples of consequent ranges

    Args:
        nums: set of numbers

    Returns:
        List of tuples of ranges
    """
    if not nums:
        return Ranges(tuple())
    nums = sorted(nums)
    gaps = [[s, e] for s, e in zip(nums, nums[1:]) if s + 1 < e]
    edges = iter(nums[:1] + sum(gaps, []) + nums[-1:])
    return Ranges(tuple(zip(edges, edges)))
