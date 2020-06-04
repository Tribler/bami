from binascii import hexlify
import orjson as json
from hashlib import sha256

KEY_LEN = 8


def key_to_id(key):
    return hexlify(key)[-KEY_LEN:].decode()


def hex_to_int(hex_val):
    return int(hexlify(hex_val), 16) % 100000000


def id_to_int(id):
    return int(id, 16)


def int_to_id(int_val):
    val = hex(int_val)[2:]
    while len(val) < KEY_LEN:
        val = "0" + val
    return val


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


def encode_frontier(frontier):
    """
    Encode to python dict
    """
    encoded = dict()
    for k, v in frontier.items():
        if k in ("h", "m", "state"):
            encoded[k] = v
        else:
            encoded[k] = encode_links(v)
    return encoded


def json_hash(value):
    return sha256(json.dumps(value)).digest()


def decode_links(link_val):
    """
    Decode to the sendable packet
    @param link_val: set of links
    """
    if type(link_val) == set:
        # set of tuples: seq_num, hash
        res = list()
        if link_val:
            for s, h in link_val:
                h_val = h.decode("utf-8") if type(h) == bytes else h
                res.append((int(s), h_val))
        return res
    else:
        return link_val


def encode_links(link_val):
    """
    Encode list of links to python set
    @param link_val: list of sendable links
    """
    res = set()
    if not link_val:
        return res
    for s, h in link_val:
        res.add((int(s), h))
    return res


def expand_ranges(range_vals):
    val_set = set()
    for b, e in range_vals:
        for val in range(b, e + 1):
            val_set.add(val)
    return val_set


def ranges(nums):
    if not nums:
        return list()
    nums = sorted(nums)
    gaps = [[s, e] for s, e in zip(nums, nums[1:]) if s + 1 < e]
    edges = iter(nums[:1] + sum(gaps, []) + nums[-1:])
    return list(zip(edges, edges))
