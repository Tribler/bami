from bami.backbone.settings import BamiSettings


class PaymentSettings(BamiSettings):
    def __init__(self):
        super().__init__()
        self.asset_precision = 10
        # Mint settings
        self.mint_value_range = (0, 100)
        self.mint_max_value = 10 ** 7
        # Spend settings
        self.spend_value_range = (0, 10 ** 7)

        # Required diversity
        self.diversity_confirm = 0
        self.should_witness_block = False

        self.block_sign_delta = 0.3
        # Maximum wait time 100
        # Maximum wait block 100
        self.max_wait_time = 100
        self.max_wait_block = 100
