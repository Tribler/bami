from typing import Dict

import pytest

from bami.backbone.block import BamiBlock
from bami.backbone.blockresponse import BlockResponseMixin, BlockResponse
from bami.backbone.community import BamiCommunity
from bami.backbone.sub_community import BaseSubCommunity, LightSubCommunity
from bami.backbone.utils import decode_raw, encode_raw
from ipv8.keyvault.crypto import default_eccrypto
from tests.mocking.base import (
    SetupValues,
    unload_nodes,
    create_and_connect_nodes,
    deliver_messages,
)


class SimpleCommunity(BamiCommunity):
    """
    A very basic community with no additional functionality. Used during the integration tests.
    """

    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        return LightSubCommunity(*args, **kwargs)

    def received_block_in_order(self, block: BamiBlock) -> None:
        pass


class BlockResponseCommunity(BlockResponseMixin, SimpleCommunity):
    """
    Basic community with block response functionality enabled.
    """

    def apply_confirm_tx(self, block: BamiBlock, confirm_tx: Dict) -> None:
        pass

    def apply_reject_tx(self, block: BamiBlock, reject_tx: Dict) -> None:
        pass

    def received_block_in_order(self, block: BamiBlock) -> None:
        print(block.transaction)
        decoded_tx = decode_raw(block.transaction)
        if decoded_tx.get(b"to_peer", None) == self.my_peer.public_key.key_to_bin():
            self.add_block_to_response_processing(block)

    def block_response(
        self, block: BamiBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        if block.type == b"good":
            return BlockResponse.CONFIRM
        elif block.type == b"bad":
            return BlockResponse.REJECT
        return BlockResponse.DELAY


@pytest.fixture
async def set_vals(tmpdir_factory, community_cls, num_nodes):
    dirs = [
        tmpdir_factory.mktemp(str(SimpleCommunity.__name__), numbered=True)
        for _ in range(num_nodes)
    ]
    nodes = create_and_connect_nodes(num_nodes, work_dirs=dirs, ov_class=community_cls)
    # Make sure every node has a community to listen to
    community_key = default_eccrypto.generate_key("curve25519").pub()
    community_id = community_key.key_to_bin()
    for node in nodes:
        node.overlay.subscribe_to_subcom(community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    for k in dirs:
        k.remove(ignore_errors=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("community_cls", [SimpleCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_simple_frontier_reconciliation_after_partition(set_vals):
    """
    Test whether missing blocks are synchronized after a network partition.
    """
    for _ in range(3):
        # Note that we do not broadcast the block to the other node
        set_vals.nodes[0].overlay.create_signed_block(com_id=set_vals.community_id)

    # Force frontier exchange
    set_vals.nodes[0].overlay.frontier_gossip_sync_task(set_vals.community_id)
    set_vals.nodes[1].overlay.frontier_gossip_sync_task(set_vals.community_id)

    await deliver_messages()

    frontier1 = (
        set_vals.nodes[0].overlay.persistence.get_chain(set_vals.community_id).frontier
    )
    frontier2 = (
        set_vals.nodes[1].overlay.persistence.get_chain(set_vals.community_id).frontier
    )
    assert len(frontier2.terminal) == 1
    assert frontier2.terminal[0][0] == 3
    assert frontier1 == frontier2


@pytest.mark.asyncio
@pytest.mark.parametrize("community_cls", [BlockResponseCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_block_confirm(set_vals):
    """
    Test whether blocks are confirmed correctly.
    """
    block = set_vals.nodes[0].overlay.create_signed_block(
        com_id=set_vals.community_id,
        transaction=encode_raw({b"to_peer": 3}),
        block_type=b"good",
    )
    set_vals.nodes[0].overlay.share_in_community(block, set_vals.community_id)
    await deliver_messages()
