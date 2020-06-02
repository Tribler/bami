from .attestation_endpoint import AttestationEndpoint
from .base_endpoint import BaseEndpoint
from .dht_endpoint import DHTEndpoint
from .isolation_endpoint import IsolationEndpoint
from .network_endpoint import NetworkEndpoint
from .noblock_dht_endpoint import NoBlockDHTEndpoint
from .overlays_endpoint import OverlaysEndpoint
from .trustchain_endpoint import TrustchainEndpoint
from .tunnel_endpoint import TunnelEndpoint


class RootEndpoint(BaseEndpoint):
    """
    The root endpoint of the HTTP API is the root resource in the request tree.
    It will dispatch requests regarding torrents, channels, settings etc to the right child endpoint.
    """

    def setup_routes(self):
        endpoints = {
            "/attestation": AttestationEndpoint,
            "/dht": DHTEndpoint,
            "/isolation": IsolationEndpoint,
            "/network": NetworkEndpoint,
            "/noblockdht": NoBlockDHTEndpoint,
            "/overlays": OverlaysEndpoint,
            "/trustchain": TrustchainEndpoint,
            "/tunnel": TunnelEndpoint,
        }
        for path, ep_cls in endpoints.items():
            self.add_endpoint(path, ep_cls())
