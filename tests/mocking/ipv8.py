from ipv8.test.mocking.ipv8 import MockIPv8


class FakeIPv8(MockIPv8):
    def __init__(self, crypto_curve, overlay_class, *args, **kwargs):
        super().__init__(crypto_curve, overlay_class, *args, **kwargs)
        self.overlays = []
        self.strategies = []
