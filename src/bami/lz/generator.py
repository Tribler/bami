# Cumulative frequency of transaction fees
import random

import numpy as np

cumulative_freq = [1378501, 3050709, 42881857, 60723356, 61219997, 61236481, 61236972, 61237045]
fee_amounts = [0, 10 ** 2, 10 ** 3, 10 ** 4, 10 ** 5, 10 ** 6, 10 ** 7, 10 ** 8]

# Normalize the cumulative frequencies
total_transactions = cumulative_freq[-1]
cumulative_freq_normalized = [freq / total_transactions for freq in cumulative_freq]


def generate_transaction_fee():
    random_value = np.random.rand()
    for i, freq in enumerate(cumulative_freq_normalized):
        if random_value <= freq:
            if i > 0:
                return random.randint(fee_amounts[i - 1], fee_amounts[i])
            else:
                return fee_amounts[i]
    return random.randint(fee_amounts[-2], fee_amounts[-1])
