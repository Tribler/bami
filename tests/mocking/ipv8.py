from tests.mocking.discovery import MockWalk
from tests.mocking.endpoint import AutoMockEndpoint
from python_project.attestation.trustchain.community import TrustChainCommunity
from python_project.dht.discovery import DHTDiscoveryCommunity
from python_project.keyvault.crypto import default_eccrypto
from python_project.messaging.interfaces.statistics_endpoint import StatisticsEndpoint
from python_project.peer import Peer
from python_project.peerdiscovery.network import Network


class MockIPv8(object):
    def __init__(
        self,
        crypto_curve,
        overlay_class,
        create_trustchain=False,
        create_dht=False,
        enable_statistics=False,
        *args,
        **kwargs
    ):
        self.endpoint = AutoMockEndpoint()
        self.endpoint.open()

        self.overlays = []
        self.strategies = []

        # Enable statistics
        if enable_statistics:
            self.endpoint = StatisticsEndpoint(self, self.endpoint)

        self.network = Network()
        self.my_peer = Peer(
            default_eccrypto.generate_key(crypto_curve), self.endpoint.wan_address
        )

        # Load a TrustChain community if specified
        self.trustchain = None
        if create_trustchain:
            self.trustchain = TrustChainCommunity(
                self.my_peer, self.endpoint, self.network, working_directory=u":memory:"
            )
            kwargs.update({"trustchain": self.trustchain})
            self.overlays.append(self.trustchain)

        # Load a DHT community if specified
        self.dht = None
        if create_dht:
            self.dht = DHTDiscoveryCommunity(self.my_peer, self.endpoint, self.network)
            kwargs.update({"dht": self.dht})
            self.overlays.append(self.dht)

        self.overlay = overlay_class(
            self.my_peer, self.endpoint, self.network, *args, **kwargs
        )
        self.overlay._use_main_thread = False
        self.overlays.append(self.overlay)
        self.discovery = MockWalk(self.overlay)

        self.overlay.my_estimated_wan = self.endpoint.wan_address
        self.overlay.my_estimated_lan = self.endpoint.lan_address

        if enable_statistics:
            self.endpoint.enable_community_statistics(self.overlay.get_prefix(), True)

    def get_overlay(self, overlay_cls):
        return next(self.get_overlays(overlay_cls), None)

    def get_overlays(self, overlay_cls):
        return (o for o in self.overlays if isinstance(o, overlay_cls))

    async def unload(self):
        self.endpoint.close()
        await self.overlay.unload()
        if self.trustchain:
            await self.trustchain.unload()
        if self.dht:
            await self.dht.unload()
