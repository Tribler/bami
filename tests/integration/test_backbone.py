from typing import Dict

import pytest

from bami.backbone.block import BamiBlock
from bami.backbone.blockresponse import BlockResponseMixin, BlockResponse
from bami.backbone.community import BamiCommunity
from bami.backbone.sub_community import BaseSubCommunity, LightSubCommunity
from bami.backbone.utils import decode_raw, encode_raw
from ipv8.keyvault.crypto import default_eccrypto
from tests.mocking.base import deliver_messages


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
def init_nodes():
    return True


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [SimpleCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_simple_frontier_reconciliation_after_partition(set_vals_by_key):
    """
    Test whether missing blocks are synchronized after a network partition.
    """
    for _ in range(3):
        # Note that we do not broadcast the block to the other node
        set_vals_by_key.nodes[0].overlay.create_signed_block(
            com_id=set_vals_by_key.community_id
        )

    # Force frontier exchange
    set_vals_by_key.nodes[0].overlay.frontier_gossip_sync_task(
        set_vals_by_key.community_id
    )
    set_vals_by_key.nodes[1].overlay.frontier_gossip_sync_task(
        set_vals_by_key.community_id
    )

    await deliver_messages()

    frontier1 = (
        set_vals_by_key.nodes[0]
        .overlay.persistence.get_chain(set_vals_by_key.community_id)
        .frontier
    )
    frontier2 = (
        set_vals_by_key.nodes[1]
        .overlay.persistence.get_chain(set_vals_by_key.community_id)
        .frontier
    )
    assert len(frontier2.terminal) == 1
    assert frontier2.terminal[0][0] == 3
    assert frontier1 == frontier2


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [BlockResponseCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_block_confirm(set_vals_by_key):
    """
    Test whether blocks are confirmed correctly.
    """
    block = set_vals_by_key.nodes[0].overlay.create_signed_block(
        com_id=set_vals_by_key.community_id,
        transaction=encode_raw({b"to_peer": 3}),
        block_type=b"good",
    )
    set_vals_by_key.nodes[0].overlay.share_in_community(
        block, set_vals_by_key.community_id
    )
    await deliver_messages()
