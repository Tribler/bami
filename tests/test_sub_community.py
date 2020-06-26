import logging

import pytest
from ipv8.keyvault.crypto import default_eccrypto
from python_project.backbone.sub_community import SubCommunityMixin, IPv8SubCommunity

from tests.mocking.community import MockSubCommunityRoutines, FakeRoutines
from tests.mocking.ipv8 import FakeIPv8


class FakeSubCommunity(SubCommunityMixin, MockSubCommunityRoutines, FakeRoutines):
    pass


def test_is_sub(monkeypatch):
    monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [b"test1"])
    f = FakeSubCommunity()
    assert f.is_subscribed(b"test1")


class TestSub:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.f = FakeSubCommunity()

    def test_one_sub_when_subscribed(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [b"test1"])
        self.f.subscribe_to_subcom(b"test1")
        assert self.f.is_subscribed(b"test1")

    def test_no_ipv8(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [])
        monkeypatch.setattr(FakeRoutines, "logger", logging.Logger(name="test"))
        f = FakeSubCommunity()
        f.subscribe_to_subcom(b"test1")

    def test_one_sub(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [])
        key = default_eccrypto.generate_key(u"medium").pub().key_to_bin()
        monkeypatch.setattr(
            FakeRoutines,
            "ipv8",
            FakeIPv8(u"curve25519", IPv8SubCommunity, subcom_id=key),
        )
        f = FakeSubCommunity()
        f.subscribe_to_subcom(b"test1")
