import csv
import logging
import time
from binascii import hexlify
from hashlib import sha1
from typing import Optional
from collections import defaultdict

from python_project.backbone.block import PlexusBlock, EMPTY_PK
from python_project.backbone.datastore.consistency import Chain
from python_project.backbone.datastore.utils import key_to_id, expand_ranges


class PlexusMemoryDatabase(object):
    """
    This class defines an optimized memory database for Plexus.
    """

    def __init__(
        self, working_directory, db_name, original_db=None, accept_all_chains=True
    ):
        self.working_directory = working_directory
        self.db_name = db_name

        self.identity_chains = dict()
        self.community_chains = dict()
        self._temp_chain_states = dict()

        # chain_id =>
        self.dumped_state = defaultdict(dict)

        self.blocks = {}
        self.block_cache = {}

        self.block_types = {}
        self.latest_blocks = {}

        self.short_map = dict()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.peer_map = {}
        self.do_commit = True

        self.block_time = {}
        self.block_file = None

        # Will reconcile and track all chains received from blocks and frontiers
        self.should_accept_all_chains = accept_all_chains

        self.original_db = None
        if original_db:
            self.original_db = original_db

            # Fill the memory database with the blocks in the original database
            blocks = original_db.get_all_blocks()
            self.logger.info("Filling memory DB with %d blocks..." % len(blocks))
            for block in blocks:
                self.add_block(block)
                peer_mid = sha1(block.public_key).digest()
                self.peer_map[peer_mid] = block.public_key

    def get_frontier(self, chain_id):
        val = self.get_peer_frontier(chain_id)
        return val if val else self.get_community_frontier(chain_id)

    def get_peer_proofs(self, chain, blk_hash, state):
        pass

    def is_state_consistent(self, chain_id):
        chain = self.get_chain(chain_id)
        return chain and chain.is_state_consistent()

    def get_state_votes(self, chain_id, seq_num=None):
        chain = self.get_chain(chain_id)
        if chain:
            return chain.state_votes.get(seq_num) if seq_num else chain.state_votes
        return None

    def add_state_vote(self, chain_id, seq_num, state_vote):
        chain = self.get_chain(chain_id)
        if not chain:
            return None
        chain.add_state_vote(seq_num, state_vote)
        return chain

    def get_state(self, chain_id, front_id, state_name=None):
        chain = self.get_chain(chain_id)
        if chain:
            return chain.get_state(front_id, state_name)
        return None

    def add_chain_state(self, chain_id, chain_state):
        if (
            chain_id not in self.community_chains
            and chain_id not in self.identity_chains
        ):
            # There no chains created => Put to temp store
            self._temp_chain_states[chain_id] = chain_state
        else:
            chain = (
                self.community_chains[chain_id]
                if chain_id in self.community_chains
                else self.identity_chains[chain_id]
            )
            chain.add_state(chain_state)

    def get_latest_max_state_votes(self, chain_id, state_name=None):
        chain = self.get_chain(chain_id)
        if chain:
            return chain.get_latest_max_votes()

    def dump_state(self, chain_id, seq_num, state):
        self.dumped_state[chain_id][seq_num] = state

    def get_state_by_hash(self, chain_id, state_hash):
        chain = self.get_chain(chain_id)
        state_ind = chain.get_state_by_hash(state_hash) if chain else None
        return (chain.get_state(state_ind), state_ind) if state_ind else None

    def get_chain(self, com_id) -> Optional[Chain]:
        if com_id not in self.community_chains and com_id not in self.identity_chains:
            return None
        return (
            self.community_chains[com_id]
            if com_id in self.community_chains
            else self.identity_chains[com_id]
        )

    def get_community_frontier(self, com_id):
        if com_id in self.community_chains:
            return self.community_chains[com_id].frontier
        return None

    def get_peer_frontier(self, peer_id):
        if peer_id in self.identity_chains:
            return self.identity_chains[peer_id].frontier
        return None

    def _create_community_chain(self, com_id):
        """
        Create if chain don't exist
        @param com_id: public key - if of the chain
        """
        if com_id not in self.community_chains:
            self.community_chains[com_id] = Chain(
                com_id, personal=False, block_store=self
            )
            if com_id in self._temp_chain_states:
                self.community_chains[com_id].add_state(
                    self._temp_chain_states.pop(com_id)
                )

    def _create_identity_chain(self, peer_id):
        """
        Create if chain don't exist
        @param com_id: public key - if of the chain
        """
        if peer_id not in self.identity_chains:
            self.identity_chains[peer_id] = Chain(peer_id, block_store=self)
            if peer_id in self._temp_chain_states:
                self.community_chains[peer_id].add_state(
                    self._temp_chain_states.pop(peer_id)
                )

    def reconcile_or_create_personal_chain(self, peer_id, frontier):
        self._create_identity_chain(peer_id)
        return self.reconcile(peer_id, frontier)

    def reconcile_or_create_community_chain(self, com_id, frontier):
        self._create_community_chain(com_id)
        return self.reconcile(com_id, frontier)

    def reconcile_or_create(self, chain_id, frontier):
        if "p" in frontier and frontier["p"]:
            return self.reconcile_or_create_personal_chain(chain_id, frontier)
        else:
            return self.reconcile_or_create_community_chain(chain_id, frontier)

    def reconcile(self, chain_id, frontier):
        if chain_id in self.community_chains:
            return self.community_chains[chain_id].reconcile(frontier)
        elif chain_id in self.identity_chains:
            return self.identity_chains[chain_id].reconcile(frontier)
        return None

    # TODO: move this to another class
    def get_block_by_short_hash(self, short_hash):
        full_hash = self.short_map.get(short_hash)
        return self.blocks.get(full_hash)

    def get_blocks_by_request(self, chain_id, request):
        blocks = set()
        chain = (
            self.identity_chains[chain_id]
            if chain_id in self.identity_chains
            else self.community_chains[chain_id]
        )
        for b_i in expand_ranges(request["m"]):
            blocks.update({self.get_block_by_short_hash(sh) for sh in chain.chain[b_i]})
        for sn, sh in request["c"]:
            val = self.get_block_by_short_hash(sh)
            if val:
                blocks.add(val)
        return blocks

    def get_block_class(self, block_type):
        """
        Get the block class for a specific block type.
        """
        if block_type not in self.block_types:
            return PlexusBlock

        return self.block_types[block_type]

    def add_peer(self, peer):
        if peer.mid not in self.peer_map:
            self.peer_map[peer.mid] = peer.public_key.key_to_bin()

    def add_block(self, block: PlexusBlock):
        """
        Add block to the database and update indexes
        @param block: PlexusBlock
        """
        if block.hash not in self.blocks:
            self.blocks[block.hash] = block
            self.short_map[key_to_id(block.hash)] = block.hash

        if block.public_key not in self.block_cache:
            # This is a public key => new user
            self.block_cache[block.public_key] = dict()

            self.short_map[key_to_id(block.public_key)] = block.public_key
            # Initialize identity chain
            self._create_identity_chain(block.public_key)
        block_id = block.sequence_number
        if block_id not in self.block_cache[block.public_key]:
            self.block_cache[block.public_key][block_id] = set()
        self.block_cache[block.public_key][block_id].add(block.hash)

        self.identity_chains[block.public_key].add_block(block)

        # Add to community chain
        if block.com_id != EMPTY_PK:
            self._create_community_chain(block.com_id)
            self.community_chains[block.com_id].add_block(block)

        # time when block is received by peer
        self.block_time[block.hash] = int(round(time.time() * 1000))

        # add to persistent
        # if self.original_db and self.do_commit:
        #    self.original_db.add_block(block)

    def remove_block(self, block):
        self.block_cache.pop((block.public_key, block.sequence_number), None)

    def get(self, public_key, sequence_number):
        if (
            public_key in self.block_cache
            and sequence_number in self.block_cache[public_key]
        ):
            return self.block_cache[public_key][sequence_number]
        return None

    def get_all_blocks(self):
        return self.blocks.values()

    def get_number_of_known_blocks(self, public_key=None):
        if public_key:
            return len([pk for pk, _ in self.block_cache.keys() if pk == public_key])
        return len(self.block_cache.keys())

    def contains(self, block):
        return block.hash in self.blocks

    def get_lastest_peer_frontier(self, peer_key):
        if peer_key in self.identity_chains:
            return self.identity_chains[peer_key].frontier
        return None

    def get_latest_community_frontier(self, com_key):
        if com_key in self.community_chains:
            return self.community_chains[com_key].frontier
        return None

    def commit_states(self, state_file):
        with open(state_file, "w") as t_file:
            writer = csv.DictWriter(t_file, ["chain_id", "last_state", "personal"])
            writer.writeheader()
            for com_key, chain in self.community_chains.items():
                state = chain.get_last_state()
                writer.writerow(
                    {
                        "chain_id": hexlify(chain.chain_id).decode(),
                        "last_state": str(state),
                        "personal": False,
                    }
                )

    def commit_block_times(self, block_file):
        with open(block_file, "w") as t_file:
            writer = csv.DictWriter(
                t_file,
                [
                    "time",
                    "transaction",
                    "type",
                    "peer_id",
                    "seq_num",
                    "com_id",
                    "com_seq",
                    "links",
                    "prevs",
                ],
            )
            writer.writeheader()
            block_ids = list(self.block_time.keys())
            for block_id in block_ids:
                block = self.blocks[block_id]
                time = self.block_time[block_id]
                from_id = hexlify(block.public_key).decode()[-8:]
                com_id = hexlify(block.com_id).decode()[:8]

                writer.writerow(
                    {
                        "time": time,
                        "transaction": str(block.transaction),
                        "type": block.type.decode(),
                        "seq_num": block.sequence_number,
                        "peer_id": from_id,
                        "com_id": com_id,
                        "com_seq": block.com_seq_num,
                        "links": str(block.links),
                        "prevs": str(block.previous),
                    }
                )
                self.block_time.pop(block_id)

    def commit(self, my_pub_key):
        """
        Commit all information to the original database.
        """
        if self.original_db:
            my_blocks = [
                self.blocks[b_hashes]
                for b_hashes in self.block_cache[my_pub_key].values()
            ]
            for block in my_blocks:
                self.original_db.add_block(block)

    def close(self):
        if self.original_db:
            self.original_db.close()
