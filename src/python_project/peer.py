from __future__ import annotations

from base64 import b64encode
from collections import deque
from struct import unpack
from time import time

from .keyvault.crypto import default_eccrypto
from .keyvault.keys import Key
from python_project.keyvault.private.libnaclkey import LibNaCLSK
from python_project.keyvault.private.m2crypto import M2CryptoSK
from python_project.keyvault.public.libnaclkey import LibNaCLPK
from typing import Tuple, Union


class Peer(object):
    def __init__(
        self,
        key: Union[bytes, LibNaCLSK, LibNaCLPK, M2CryptoSK],
        address: Tuple[str, int] = ("0.0.0.0", 0),
        intro: bool = True,
    ) -> None:
        """
        Create a new Peer.

        :param key: the peer's Key (mostly public) or public key bin
        :param lan_address: the (IP, port) tuple of this peer on its LAN
        :param wan_address: the (IP, port) tuple of this peer on its WAN
        :param intro: is this peer suggested to us (otherwise it contacted us)
        """
        if not isinstance(key, Key):
            self.key = default_eccrypto.key_from_public_bin(key)
        else:
            self.key = key
        self.mid = self.key.key_to_hash()
        self.public_key = self.key.pub()
        self.address = address
        self.last_response = 0 if intro else time()
        self._lamport_timestamp = 0
        self.pings = deque(maxlen=5)

    def get_median_ping(self):
        """
        Get the median ping time of this peer.

        :return: the median ping or None if no measurements were performed yet
        :rtype: float or None
        """
        if not self.pings:
            return None
        sorted_pings = sorted(self.pings)
        if len(sorted_pings) % 2 == 0:
            return (
                sorted_pings[len(sorted_pings) // 2 - 1]
                + sorted_pings[len(sorted_pings) // 2]
            ) / 2
        else:
            return sorted_pings[len(sorted_pings) // 2]

    def get_average_ping(self):
        """
        Get the average ping time of this peer.

        :return: the average ping or None if no measurements were performed yet
        :rtype: float or None
        """
        if not self.pings:
            return None
        return sum(self.pings) / len(self.pings)

    def update_clock(self, timestamp: int) -> None:
        """
        Update the Lamport timestamp for this peer. The Lamport clock dictates that the current timestamp is
        the maximum of the last known and the most recently delivered timestamp. This is useful when messages
        are delivered asynchronously.

        We also keep a real time timestamp of the last received message for timeout purposes.

        :param timestamp: a received timestamp
        """
        self._lamport_timestamp = max(self._lamport_timestamp, timestamp)
        self.last_response = time()  # This is in seconds since the epoch

    def get_lamport_timestamp(self) -> int:
        return self._lamport_timestamp

    def __hash__(self) -> int:
        (as_long,) = unpack(">Q", self.mid[:8])
        return as_long

    def __eq__(self, other: Peer) -> bool:
        if not isinstance(other, Peer):
            return False
        return self.public_key.key_to_bin() == other.public_key.key_to_bin()

    def __ne__(self, other: Peer) -> bool:
        if not isinstance(other, Peer):
            return True
        return self.public_key.key_to_bin() != other.public_key.key_to_bin()

    def __str__(self) -> str:
        return "Peer<%s:%d, %s>" % (
            self.address[0],
            self.address[1],
            b64encode(self.mid).decode("utf-8"),
        )
