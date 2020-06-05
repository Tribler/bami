from ipv8.attestation.trustchain.community import TrustChainCommunity


class NoodleCommunity(TrustChainCommunity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.observanda = set()

    async def witnessing_lc(self):
        for p in self.observanda:
            # Send crawl request, getting the info
            # Get latest block of a peer
            last_sn = self.persistence.get_latest(p.public_key.key_to_bin())
            self.crawl_chain(p, last_sn)

        # Witnessing duty
        # Poss a challenge to the observandum => based on the previous knowledge

        # The witnesses are assigned/asked by other peers.

        # reconcile with the peer that are interested in the

    def reconcile(self, peer, topic):
        """
        Reconcile information with peer about the account 'topic'
        """
        # Get the latest information about the
        pass

    def create_block(self):
        # Create block with required transaction
        # Add witnessing links to latest known and verified blocks
        # Create a blocks and notify other peers? - with a reconciliation mechanism.
        pass
