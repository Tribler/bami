import pytest

from bami.basalt.peer import BasaltPeer
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer


@pytest.fixture
def key():
    return default_eccrypto.generate_key("curve25519")


@pytest.fixture
def peer(key):
    return BasaltPeer(key, address=("1.2.3.4", 12345))


def test_peer_conversion(key):
    ipv8_peer = Peer(key, address=("3.4.5.6", 12345))
    basalt_peer = BasaltPeer.from_peer(ipv8_peer)

    assert ipv8_peer.address == basalt_peer.address
    assert ipv8_peer.key == basalt_peer.key


def test_ip_prefix(peer):
    assert peer.get_ip_prefix_8() == "1"
    assert peer.get_ip_prefix_16() == "1.2"
    assert peer.get_ip_prefix_24() == "1.2.3"
