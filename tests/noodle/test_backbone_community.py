from asyncio import sleep

from .test_consistency_chain import MockChainState
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.test.base import TestBase

from python_project.backbone.block import PlexusBlock
from python_project.backbone.community import PlexusCommunity
from python_project.backbone.settings import PlexusSettings

from tests.noodle.mocking.fake_ipv8 import FakeIPv8


class DummyBlock(PlexusBlock):
    """
    This dummy block is used to verify the conversion to a specific block class during the tests.
    Other than that, it has no purpose.
    """

    pass


class TestPlexusCommunityBase(TestBase):
    __testing__ = False
    NUM_NODES = 2

    def setUp(self):
        super(TestPlexusCommunityBase, self).setUp()
        self.initialize(PlexusCommunity, self.NUM_NODES)

        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        for node in self.nodes:
            node.overlay.subscribe_to_community(self.community_id)

    def create_node(self):
        settings = PlexusSettings()
        ipv8 = FakeIPv8(u"curve25519", PlexusCommunity, working_directory=u":memory:")
        ipv8.overlay.ipv8 = ipv8

        return ipv8

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
            transaction=b'',
        )
        await self.deliver_messages()

        # Node 1 should now have the block in its database
        self.assertTrue(
            self.nodes[1].overlay.persistence.get(
                block.public_key, block.sequence_number
            )
        )


class TestPlexusCommunityTwoNodes(TestPlexusCommunityBase):
    __testing__ = True

    async def test_basic_horizontal_chain_no_conclict_one_tx(self):
        """
        Test a very basic horizontal chain where one node creates a block in a horizontal community.
        """
        # Create a new block now in that community
        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )

        await sleep(1)
        self.assertTrue(
            self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        )

        self.assertTrue(
            self.nodes[1].overlay.persistence.get_frontier(self.community_id)
        )

    async def test_basic_horizontal_chain_no_conclict_two_txs(self):
        """
        Test a very basic horizontal chain where one node creates a block in a horizontal community,
        and another node builds upon that.
        """
        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        await sleep(1)

        block = await self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        self.assertTrue(block.links)
        await sleep(1)

        # The frontier should now be the block created by peer 1
        frontier = self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        self.assertFalse(frontier["p"])
        self.assertTrue(frontier["v"])

    async def test_basic_horizontal_chain_no_conclict_three_txs(self):
        """
        Test a very basic horizontal chain where nodes creates a block in a horizontal community simultaneously,
        and another node builds upon that.
        """
        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        await sleep(1)

        # The frontier should now be two blocks
        frontier = self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(len(list(frontier["v"])), 2)

        block = await self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        self.assertEqual(len(list(block.links)), 2)
        await sleep(1)

        # The frontier should now be the block created by peer 1
        frontier = self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(len(list(frontier["v"])), 1)

    async def test_basic_horizontal_chain_conclict_three_txs(self):
        """
        Test a basic horizontal chain with conflicts. The final result should be a frontier consisting of two blocks.
        """

        await self.introduce_nodes()

        self.nodes[0].endpoint.close()
        self.nodes[1].endpoint.close()

        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'',
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )
        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )

        self.nodes[0].endpoint.open()
        self.nodes[1].endpoint.open()

        await sleep(2)  # This requires two rounds for reconciliation (each in 1 second)

        frontier_a = self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        frontier_b = self.nodes[1].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(frontier_a, frontier_b)


class TestPlexusCommunityThreeNodes(TestPlexusCommunityBase):
    __testing__ = True
    NUM_NODES = 3

    async def test_basic_horizontal_chain_three_tx(self):
        """
        With 3 peers, check that one peer builds upon the frontier containing the blocks of the other peers.
        """

        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )

        await sleep(1)

        self.nodes[1].overlay.sign_block(
            self.nodes[2].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test"
        )

        await sleep(1)

        # The frontier should be the last block created by peer 2
        frontier = self.nodes[2].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(len(list(frontier["v"])), 1)
        frontier = self.nodes[1].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(1, len(list(frontier["v"])))

    async def test_stateful_horizontal_chain(self):
        for i in range(3):
            self.nodes[i].overlay.persistence.add_chain_state(
                self.community_id, MockChainState("sum")
            )

        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 40}',
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 45}',
        )

        await sleep(1)

        self.nodes[1].overlay.sign_block(
            self.nodes[2].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 50}',
        )

        await sleep(1)

        # The frontier should be the last block created by peer 2
        frontier = self.nodes[2].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(len(list(frontier["v"])), 1)
        frontier = self.nodes[1].overlay.persistence.get_frontier(self.community_id)

        self.assertEqual(1, len(list(frontier["v"])))

        val = self.nodes[1].overlay.persistence.get_state(self.community_id, 2)
        val1 = self.nodes[0].overlay.persistence.get_state(self.community_id, 2)
        val2 = self.nodes[2].overlay.persistence.get_state(self.community_id, 2)

        self.assertEqual(val, val1)
        self.assertEqual(val1, val2)

    async def test_state_messages_horizontal_chain(self):
        for i in range(2):
            self.nodes[i].overlay.persistence.add_chain_state(
                self.community_id, MockChainState("sum")
            )

        self.nodes[0].overlay.sign_block(
            self.nodes[0].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 40}',
        )
        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 45}',
        )

        await sleep(1)

        self.nodes[1].overlay.sign_block(
            self.nodes[1].overlay.my_peer,
            com_id=self.community_id,
            block_type=b"test",
            transaction=b'{"id": 50}',
        )

        await sleep(1)

        # The frontier should be the last block created by peer 2
        frontier = self.nodes[0].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(len(list(frontier["v"])), 1)

        frontier = self.nodes[1].overlay.persistence.get_frontier(self.community_id)
        self.assertEqual(1, len(list(frontier["v"])))

        val = self.nodes[1].overlay.persistence.get_state(self.community_id, 2)
        val1 = self.nodes[0].overlay.persistence.get_state(self.community_id, 2)

        # Node 2 request block from node 0
        p_adr = self.nodes[1].overlay.my_peer.address
        self.nodes[2].overlay.request_state(p_adr, self.community_id)

        await sleep(0.2)
        val2 = (
            self.nodes[2].overlay.persistence.dumped_state.get(self.community_id).get(2)
        )

        self.assertEqual(val, val1)
        self.assertEqual(val1, val2)
