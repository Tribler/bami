from __future__ import annotations

import struct
from typing import Optional, Tuple, Union

from python_project.attestation.wallet.primitives.value import FP2Value

__all__ = [
    "ipack",
    "iunpack",
    "pack_pair",
    "unpack_pair",
    "BonehPublicKey",
    "BonehPrivateKey",
]


def _num_to_str(num: int) -> bytes:
    """
    Convert an integer to a str.
    """
    out = b""
    h = hex(num)[2:]
    if h.endswith("L"):
        h = h[:-1]
    if (len(h) % 2) == 1:
        h = "0" + h
    for b in range(0, len(h), 2):
        out += struct.pack(">B", int(h[b] + h[b + 1], 16))
    return out


def _str_to_num(s: bytes) -> int:
    """
    Convert a str to an integer.
    """
    out = 0
    for i in range(len(s)):
        out <<= 8
        out |= struct.unpack(">B", s[i : i + 1])[0]
    return out


def ipack(num: int) -> bytes:
    """
    Serialize an integer.
    """
    pnum = _num_to_str(num)
    l = _num_to_str(len(pnum))
    return struct.pack(">B", len(l)) + l + pnum


def iunpack(s: bytes) -> Tuple[int, bytes]:
    """
    Unserialize an integer from a str.
    """
    llen = struct.unpack(">B", s[0:1])[0]
    l = _str_to_num(s[1 : 1 + llen])
    return _str_to_num(s[1 + llen : llen + l + 1]), s[llen + l + 1 :]


def pack_pair(a: int, b: int) -> bytes:
    """
    Serialize a pair of two integers.
    """
    return ipack(a) + ipack(b)


def unpack_pair(s: bytes) -> Tuple[int, int, bytes]:
    """
    Unserialize a pair of two integers.
    """
    a, r = iunpack(s)
    b, r = iunpack(r)
    return a, b, r


class BonehPublicKey(object):
    """
    A public key for Boneh et al.'s cryptosystem.
    """

    FIELDS = 5

    def __init__(self, p: int, g: FP2Value, h: FP2Value) -> None:
        self.p = p
        self.g = g
        self.h = h

    def serialize(self) -> bytes:
        return (
            ipack(self.p)
            + ipack(self.g.a)
            + ipack(self.g.b)
            + ipack(self.h.a)
            + ipack(self.h.b)
        )

    @classmethod
    def unserialize(cls, s: bytes) -> Optional[Union[BonehPrivateKey, BonehPublicKey]]:
        rem = s
        nums = []
        while rem and len(nums) < cls.FIELDS:
            unpacked, rem = iunpack(rem)
            nums.append(unpacked)
        if len(nums) != cls.FIELDS:
            return None
        inits = [
            nums[0],
            FP2Value(nums[0], nums[1], nums[2]),
            FP2Value(nums[0], nums[3], nums[4]),
        ]
        if len(nums) > 5:
            inits.append(nums[5])
            inits.append(nums[6])
        return cls(*inits)


class BonehPrivateKey(BonehPublicKey):
    """
    A private key for Boneh et al.'s cryptosystem.
    """

    FIELDS = 7

    def __init__(self, p: int, g: FP2Value, h: FP2Value, n: int, t1: int) -> None:
        super(BonehPrivateKey, self).__init__(p, g, h)
        self.n = n
        self.t1 = t1

    def serialize(self) -> bytes:
        return super(BonehPrivateKey, self).serialize() + ipack(self.n) + ipack(self.t1)

    def public_key(self) -> BonehPublicKey:
        return BonehPublicKey(self.p, self.g, self.h)
