import numpy as np


class FeeGenerator:
    def __init__(self):
        # Cumulative frequency of transaction fees
        self.cumulative_freq = [1378501, 3050709, 42881857, 60723356, 61219997, 61236481, 61236972, 61237045]
        self.fee_amounts = [0, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10]
        self.fee_amounts = [int(f * 100000) for f in self.fee_amounts]

        # Normalize the cumulative frequencies
        self.total_transactions = self.cumulative_freq[-1]
        self.cumulative_freq_normalized = [freq / self.total_transactions for freq in self.cumulative_freq]

    def generate_fee(self):
        random_value = np.random.rand()
        for i, freq in enumerate(self.cumulative_freq_normalized):
            if random_value <= freq:
                return self.fee_amounts[i]
        return self.fee_amounts[-1]