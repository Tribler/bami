from binascii import unhexlify
from typing import Any, Optional

from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.messaging.payload import Payload
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from ipv8.test.base import TestBase
from python_project.backbone.community_routines import CommunityRoutines
from python_project.backbone.datastore.database import BaseDB

from tests.mocking.ipv8 import FakeIPv8


class MyCommunity(CommunityRoutines, Community):
    master_peer = Peer(
        unhexlify(
            "4c69624e61434c504b3a062780beaeb40e70fca4cfc1b7751d734f361cf8d815db24dbb8a99fc98af4"
            "39fc977d84f71a431f8825ba885a5cf86b2498c6b473f33dd20dbdcffd199048fc"
        )
    )

    @property
    def settings(self) -> Any:
        pass

    def send_packet(self, peer: Peer, packet: Payload, sig: bool = True) -> None:
        pass

    @property
    def persistence(self) -> BaseDB:
        pass

    @property
    def request_cache(self) -> RequestCache:
        pass


class TestPlexusCommunityBase(TestBase):
    __testing__ = False
    NUM_NODES = 2

    def setUp(self):
        super(TestPlexusCommunityBase, self).setUp()
        self.initialize(MyCommunity, self.NUM_NODES)

        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        for node in self.nodes:
            node.overlay.subscribe_to_subcom(self.community_id)

    def create_node(self):
        ipv8 = FakeIPv8(u"curve25519", MyCommunity,)
        ipv8.overlay.ipv8 = ipv8

        return ipv8

    def test_subscribe(self):
        assert self.nodes[0].overlay.is_subscribed(self.community_id)
        assert self.nodes[1].overlay.is_subscribed(self.community_id)
