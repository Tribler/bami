from bami.basalt.community import BasaltCommunity
from bami.basalt.settings import BasaltSettings
from ipv8.test.base import TestBase
from ipv8.test.mocking.ipv8 import MockIPv8


class TestBasaltCommunity(TestBase):
    NUM_NODES = 5

    def setUp(self):
        super(TestBasaltCommunity, self).setUp()
        self.initialize(BasaltCommunity, self.NUM_NODES)

    def create_node(self):
        settings = BasaltSettings()
        settings.view_size = 1
        settings.min_bootstrap_peers = self.NUM_NODES - 1
        return MockIPv8("curve25519", BasaltCommunity, settings=settings)

    async def test_peer_sampling(self):
        has_sample = [False] * self.NUM_NODES

        def on_sample(peer_ind, sampled_peer):
            assert sampled_peer
            has_sample[peer_ind] = True

        for peer_ind, node in enumerate(self.nodes):
            node.overlay.sample_callback = lambda sampled_peer, ind=peer_ind: on_sample(
                ind, sampled_peer
            )
            node.overlay.check_sufficient_peers()

        # Update views
        for node in self.nodes:
            node.overlay.on_basalt_tick()
            node.overlay.peer_update()

        await self.deliver_messages()

        assert all(has_sample)
