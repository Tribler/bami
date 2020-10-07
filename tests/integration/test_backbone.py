from typing import Dict, Any, Optional

import pytest

from bami.backbone.block import BamiBlock
from bami.backbone.community import BlockResponse, BamiCommunity
from bami.backbone.sub_community import BaseSubCommunity, LightSubCommunity
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

    def apply_confirm_tx(self, block: BamiBlock, confirm_tx: Dict) -> None:
        pass

    def apply_reject_tx(self, block: BamiBlock, reject_tx: Dict) -> None:
        pass

    def block_response(
        self, block: BamiBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        return BlockResponse.DELAY

    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        return LightSubCommunity(*args, **kwargs)

    def received_block_in_order(self, block: BamiBlock) -> None:
        pass


NUM_NODES = 2


@pytest.fixture
async def set_vals(tmpdir_factory):
    dirs = [
        tmpdir_factory.mktemp(str(SimpleCommunity.__name__), numbered=True)
        for _ in range(NUM_NODES)
    ]
    nodes = create_and_connect_nodes(
        NUM_NODES, work_dirs=dirs, ov_class=SimpleCommunity
    )
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
