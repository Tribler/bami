class BasaltSettings:
    def __init__(self):

        # Algorithm parameters
        self.view_size = 20  # v
        self.replacement_count = 10  # k
        self.sampling_rate = 1  # rho
        self.min_bootstrap_peers = 10
        self.time_unit_in_seconds = 1

        # Whether we should automatically start the Basalt logic upon community initialization
        self.auto_start_logic = True
