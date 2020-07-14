import pytest
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.test.mocking.endpoint import internet
from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.chain_store import Frontier
from python_project.backbone.datastore.utils import Links, Ranges
from python_project.backbone.settings import PlexusSettings

from tests.mocking.base import TestBase
from tests.mocking.community import FakeBackCommunity
from tests.mocking.ipv8 import FakeIPv8


class DummyBlock(PlexusBlock):
    """
    This dummy block is used to verify the conversion to a specific block class during the tests.
    Other than that, it has no purpose.
    """

    pass


class TestBackBoneCommunity(TestBase):
    __testing__ = False
    NUM_NODES = 2

    @pytest.yield_fixture(autouse=True)
    def main_fix(self):
        self.nodes = []
        internet.clear()
        self._tempdirs = []

        super().setUp()
        self.initialize(FakeBackCommunity, self.NUM_NODES)

        # TODO: Add additional setup
        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        for node in self.nodes:
            node.overlay.subscribe_to_subcom(self.community_id)
        yield

    # Test share in community
    # Confirm block
    # Reject block

    # TestIntroduction


    def test_init_setup(self):
        assert chr(RawBlockBroadcastPayload.msg_id) in self.nodes[0].overlay.decode_map
        assert chr(BlockBroadcastPayload.msg_id) in self.nodes[0].overlay.decode_map


    def test_subscribe(self):
        assert self.nodes[0].overlay.is_subscribed(self.community_id)
        assert self.nodes[1].overlay.is_subscribed(self.community_id)

    async def test_received_frontier(self):
        chain_id = b"chain_id"
        term = Links(((1, "0303003"),))
        frontier = Frontier(term, Ranges(()), ())
        self.nodes[0].overlay.send_frontier(
            chain_id, frontier, [self.nodes[1].overlay.my_peer]
        )
        await self.deliver_messages()

    async def test_basic_vertical_chain_sync(self):
        """
        Check whether two parties can track each others vertical chains
        """
        self.nodes[0].overlay.settings.track_neighbours_chains = True
        self.nodes[1].overlay.settings.track_neighbours_chains = True
        await self.introduce_nodes()

        # Have node 0 create a block
        block = await self.nodes[0].overlay.sign_block(
            list(self.nodes[0].network.verified_peers)[0],
            block_type=b"test",
            transaction=b"",
        )
        # await self.deliver_messages()

        # Node 1 should now have the block in its database
        # self.assertTrue(
        #    self.nodes[1].overlay.persistence.get(
        #        block.public_key, block.sequence_number
        #    )
        # )


# TODO: Test subscribe multiple communities

