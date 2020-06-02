from python_project.peerdiscovery.discovery import DiscoveryStrategy


class MockWalk(DiscoveryStrategy):
    def take_step(self) -> None:
        for peer in self.overlay.network.verified_peers:
            self.overlay.walk_to(peer.address)
