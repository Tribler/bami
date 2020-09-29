from __future__ import annotations

from abc import ABCMeta
from asyncio import ensure_future, PriorityQueue, sleep
from binascii import hexlify
from collections import defaultdict
from decimal import Decimal
from random import Random
from typing import Any, Dict, Optional, Tuple

from bami.backbone.block import BamiBlock
from bami.backbone.community import BamiCommunity, BlockResponse
from bami.backbone.exceptions import InvalidTransactionFormatException
from bami.backbone.utils import (
    CONFIRM_TYPE,
    decode_raw,
    Dot,
    encode_raw,
    hex_to_int,
    REJECT_TYPE,
    shorten,
    take_hash,
    WITNESS_TYPE,
)
from bami.payment.database import ChainState, PaymentState
from bami.payment.exceptions import (
    InsufficientBalanceException,
    InvalidMintRangeException,
    InvalidSpendRangeException,
    InvalidWitnessTransactionException,
    UnboundedMintException,
    UnknownMinterException,
)
from bami.payment.settings import PaymentSettings
from bami.payment.utils import MINT_TYPE, SPEND_TYPE
import cachetools

"""
Exchange of the value within one community, where value lives only in one community.
 - The community has the identity with the key of the master peer.
 - Master peers is the only peer that can create value
 - Other peers verify transactions created and linked to the main log.
 - Every transaction created by the master peer must be relayed to each other, creating a linear log.
 - Claim transactions
 - Witnessing transactions are linked to other frontier transactions and are collected further.
"""


class PaymentCommunity(BamiCommunity, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # self.transfer_queue = Queue()
        # self.transfer_queue_task = ensure_future(self.evaluate_transfer_queue())

        # Add state db
        if not kwargs.get("settings"):
            self._settings = PaymentSettings()
        self.state_db = PaymentState(self._settings.asset_precision)

        self.context = self.state_db.context

        self.reachability_cache = defaultdict(lambda: cachetools.LRUCache(100))

        # Dictionary chain_id: block_dot -> block
        self.tracked_blocks = defaultdict(lambda: {})
        self.peer_conf = defaultdict(lambda: defaultdict(int))
        self.should_witness_subcom = {}

        self.counter_signing_block_queue = PriorityQueue()
        self.block_sign_queue_task = ensure_future(
            self.evaluate_counter_signing_blocks()
        )

        self.witness_delta = kwargs.get("witness_delta")
        if not self.witness_delta:
            self.witness_delta = self.settings.witness_block_delta

    @property
    def settings(self) -> PaymentSettings:
        return super().settings

    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        # 0. Add master peer to the known minter group
        self.state_db.add_known_minters(sub_com_id, {sub_com_id})

        # 1. Main payment chain: spends and their confirmations
        # - Start gossip sync task periodically on the chain updates
        self.start_gossip_sync(sub_com_id)
        # - Process incoming blocks on the chain in order for payments
        self.subscribe_in_order_block(sub_com_id, self.received_block_in_order)

        # 2. Witness chain:
        # - Gossip witness updates on the sub-chain
        self.start_gossip_sync(sub_com_id, prefix=b"w")
        # - Process witness block out of order
        self.subscribe_out_order_block(b"w" + sub_com_id, self.process_witness_block)
        # - Witness all updates on payment chain
        self.should_witness_subcom[sub_com_id] = self.settings.should_witness_block

    def received_block_in_order(self, block: BamiBlock) -> None:
        if block.com_dot in self.state_db.applied_dots:
            raise Exception(
                "Block already applied?",
                block.com_dot,
                self.state_db.vals_cache,
                self.state_db.peer_mints,
                self.state_db.applied_dots,
            )
        chain_id = block.com_id
        dot = block.com_dot
        self.state_db.applied_dots.add(dot)

        # Check reachability for target block -> update risk
        for blk_dot in self.tracked_blocks[chain_id]:
            if self.dot_reachable(chain_id, blk_dot, dot):
                self.update_risk(chain_id, block.public_key, blk_dot[0])

        # Process blocks according to their type
        self.logger.debug(
            "Processing block (type: %s, chain ID: %s, hash: %s)", block.type, chain_id, hexlify(block.hash).decode()
        )
        if block.type == MINT_TYPE:
            self.process_mint(block)
        elif block.type == SPEND_TYPE:
            self.process_spend(block)
        elif block.type == CONFIRM_TYPE:
            self.process_confirm(block)
        elif block.type == REJECT_TYPE:
            self.process_reject(block)
        elif block.type == WITNESS_TYPE:
            raise Exception("Witness block received, while shouldn't")
        # Witness block react on new block:
        if (
            self.should_witness_subcom.get(chain_id)
            and block.type != WITNESS_TYPE
            and self.should_witness_chain_point(
                chain_id, self.my_pub_key_bin, block.com_seq_num
            )
        ):
            self.schedule_witness_block(chain_id, block.com_seq_num)

    def process_witness_block(self, blk: BamiBlock) -> None:
        """Process witness block out of order"""
        # No block is processed out of order in this community
        self.logger.debug(
            "Processing block %s, %s, %s", blk.type, blk.com_dot, blk.com_id
        )
        if blk.type != WITNESS_TYPE:
            raise Exception("Received not witness block on witness sub-chain!")
        self.process_witness(blk)

    def should_store_store_update(self, chain_id: bytes, seq_num: int) -> bool:
        """Store the status of the chain at the seq_num for further witnessing or verification"""
        # Should depend on if witnessing? - or something different
        return True
        # return self.should_witness_chain_point(chain_id, self.my_pub_key_bin, seq_num)

    def should_witness_chain_point(
        self, chain_id: bytes, peer_id: bytes, seq_num: int
    ) -> bool:
        """
        Returns:
            True if peer should witness this chain at seq_num
        """
        # Based on random coin tossing?
        seed = chain_id + peer_id + bytes(seq_num)
        ran = Random(seed)

        # Every peer should witness every K blocks?
        # TODO: that should depend on the number of peers in the community - change that
        # + account for the fault tolerance
        if ran.random() < 1 / self.witness_delta:
            return True
        return False

    # -------------- Mint transaction ----------------------

    def verify_mint(
        self, chain_id: bytes, minter: bytes, mint_transaction: Dict
    ) -> None:
        """
        Verify that mint transaction from minter is valid:
            - minter is known and acceptable
            - mint if properly formatted
            - mint value is withing the acceptable range
            - total value minted by the minter is limited
        Args:
            chain_id: chain identifier
            minter: id of the minter, e.g. public key
            mint_transaction: transaction as a dictionary
        Raises:
            InvalidMintException if not valid mint
        """
        # 1. Is minter known and acceptable?
        if not self.state_db.known_chain_minters(
            chain_id
        ) or minter not in self.state_db.known_chain_minters(chain_id):
            raise UnknownMinterException(
                "Got minting from unacceptable peer ", chain_id, minter
            )
        # 2. Mint if properly formatted
        if not mint_transaction.get(b"value"):
            raise InvalidTransactionFormatException(
                "Mint transaction badly formatted ", mint_transaction, chain_id, minter
            )
        # 3. Minting value within the range
        if not (
            Decimal(self.settings.mint_value_range[0], self.context)
            < mint_transaction[b"value"]
            < Decimal(self.settings.mint_value_range[1], self.context)
        ):
            raise InvalidMintRangeException(
                chain_id, minter, mint_transaction.get(b"value")
            )
        # 4. Total value is bounded
        if not (
            self.state_db.peer_mints[minter]
            + Decimal(mint_transaction.get(b"value"), self.context)
            < Decimal(self.settings.mint_max_value, self.context)
        ):
            raise UnboundedMintException(
                chain_id,
                minter,
                self.state_db.peer_mints[minter],
                mint_transaction.get(b"value"),
            )

    def mint(self, value: Decimal = None, chain_id: bytes = None) -> None:
        """
        Create mint for own reputation: Reputation & Liveness  at Stake
        """
        if not value:
            value = self.settings.initial_mint_value
        if not chain_id:
            # Community id is the same as the peer id
            chain_id = self.my_pub_key_bin
        # Mint transaction: value
        mint_tx = {b"value": float(value)}
        self.verify_mint(chain_id, self.my_pub_key_bin, mint_tx)
        block = self.create_signed_block(
            block_type=MINT_TYPE, transaction=encode_raw(mint_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def process_mint(self, mint_blk: BamiBlock) -> None:
        """Process received mint transaction"""
        minter = mint_blk.public_key
        mint_tx = decode_raw(mint_blk.transaction)
        chain_id = mint_blk.com_id
        mint_dot = mint_blk.com_dot
        prev_links = mint_blk.links
        self.verify_mint(chain_id, minter, mint_tx)

        seq_num = mint_dot[0]
        self.state_db.apply_mint(
            chain_id,
            mint_dot,
            prev_links,
            minter,
            Decimal(mint_tx.get(b"value"), self.context),
            self.should_store_store_update(chain_id, seq_num),
        )

    # ------ Spend transaction -----------
    def spend(
        self,
        chain_id: bytes,
        counter_party: bytes,
        value: Decimal,
        ignore_validation: bool = False,
    ) -> None:
        """
        Spend tokens in the chain to the counter_party.
        Args:
            chain_id: identity of the chain
            counter_party: identity of the counter-party
            value: Decimal value to transfer
            ignore_validation: if True and balance is negative - will raise an Exception
        """
        bal = self.state_db.get_balance(self.my_pub_key_bin)
        if ignore_validation or bal - value >= 0:
            spend_tx = {
                b"value": float(value),
                b"to_peer": counter_party,
                b"prev_pairwise_link": self.state_db.get_last_pairwise_links(
                    self.my_pub_key_bin, counter_party
                ),
            }
            self.verify_spend(self.my_pub_key_bin, spend_tx)
            block = self.create_signed_block(
                block_type=SPEND_TYPE, transaction=encode_raw(spend_tx), com_id=chain_id
            )
            self.logger.info("Created spend block %s", block.com_dot)
            counter_peer = self.get_peer_by_key(counter_party, chain_id)
            if counter_peer:
                self.send_block(block, [counter_peer])
            self.share_in_community(block, chain_id)
        else:
            raise InsufficientBalanceException("Not enough balance for spend")

    def verify_spend(self, spender: bytes, spend_transaction: Dict) -> None:
        """Verify the spend transaction:
            - spend formatted correctly
        Raises:
            InvalidTransactionFormat
        """
        # 1. Verify the spend format
        if (
            not spend_transaction.get(b"value")
            or not spend_transaction.get(b"to_peer")
            or not spend_transaction.get(b"prev_pairwise_link")
        ):
            raise InvalidTransactionFormatException(
                "Spend transaction badly formatted ", spender, spend_transaction
            )
        # 2. Verify the spend value in range
        if not (
            self.settings.spend_value_range[0]
            < spend_transaction.get(b"value")
            < self.settings.spend_value_range[1]
        ):
            raise InvalidSpendRangeException(
                "Spend value out of range", spender, spend_transaction.get(b"value")
            )

    def process_spend(self, spend_block: BamiBlock) -> None:
        # Store spend in the database
        spend_tx = decode_raw(spend_block.transaction)
        spender = spend_block.public_key
        self.verify_spend(spender, spend_tx)

        chain_id = spend_block.com_id
        spend_dot = spend_block.com_dot
        pers_links = spend_block.links

        prev_spend_links = spend_tx.get(b"prev_pairwise_link")
        value = Decimal(spend_tx.get(b"value"), self.context)
        to_peer = spend_tx.get(b"to_peer")
        seq_num = spend_dot[0]

        self.state_db.apply_spend(
            chain_id,
            prev_spend_links,
            pers_links,
            spend_dot,
            spender,
            to_peer,
            value,
            self.should_store_store_update(chain_id, seq_num),
        )

        # Is this block related to my peer?
        if to_peer == self.my_pub_key_bin:
            self.add_block_to_response_processing(spend_block)

    # ------------ Block Response processing ---------

    def add_block_to_response_processing(self, block: BamiBlock) -> None:
        self.tracked_blocks[block.com_id][block.com_dot] = block

        self.counter_signing_block_queue.put_nowait((block.com_seq_num, (0, block)))

    def process_counter_signing_block(
        self, block: BamiBlock, time_passed: float = None, num_block_passed: int = None,
    ) -> bool:
        """
        Process block that should be counter-signed and return True if the block should be delayed more.
        Args:
            block: Processed block
            time_passed: time passed since first added
            num_block_passed: number of blocks passed since first added
        Returns:
            Should add to queue again.
        """
        res = self.block_response(block, time_passed, num_block_passed)
        if res == BlockResponse.CONFIRM:
            self.confirm(
                block,
                extra_data={b"value": decode_raw(block.transaction).get(b"value")},
            )
            return False
        elif res == BlockResponse.REJECT:
            self.reject(block)
            return False
        return True

    async def evaluate_counter_signing_blocks(self, delta: float = None):
        while True:
            _delta = delta if delta else self.settings.block_sign_delta
            priority, block_info = await self.counter_signing_block_queue.get()
            process_time, block = block_info
            should_delay = self.process_counter_signing_block(block, process_time)
            self.logger.debug(
                "Processing counter signing block. Delayed: %s", should_delay
            )
            if should_delay:
                self.counter_signing_block_queue.put_nowait(
                    (priority, (process_time + _delta, block))
                )
                await sleep(_delta)
            else:
                self.tracked_blocks[block.com_id].pop(block.com_dot)
                await sleep(0.001)

    def block_response(
        self, block: BamiBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        # Analyze the risk of accepting this block
        stat = self.state_db.get_closest_peers_status(block.com_id, block.com_seq_num)
        # If there is no information or chain is forked or
        peer_id = shorten(block.public_key)

        if not stat or not stat[1].get(peer_id):
            # Check that it is not infinite
            if (wait_time and wait_time > self.settings.max_wait_time) or (
                wait_blocks and wait_blocks > self.settings.max_wait_block
            ):
                return BlockResponse.REJECT
            return BlockResponse.DELAY
        if not stat[1][peer_id][1] or not stat[1][peer_id][0]:
            # If chain is forked or negative balance => reject
            return BlockResponse.REJECT

        # Verify the risk of missing some information:
        #  - There is diverse peers building upon the block

        # TODO: revisit that - number should depend on total number of peers in community.
        # 1. Diversity on the block building
        f = self.settings.diversity_confirm

        if len(self.peer_conf[(block.com_id, block.com_seq_num)]) >= f:
            return BlockResponse.CONFIRM
        else:
            return BlockResponse.DELAY

    def dot_reachable(self, chain_id: bytes, target_dot: Dot, block_dot: Dot):
        val = self.reachability_cache[(chain_id, target_dot)].get(block_dot)
        if val is not None:
            return val
        res = self.persistence.get_chain(chain_id).get_prev_links(block_dot)
        if target_dot in res:
            return True
        if max(res)[0] < target_dot[0]:
            return False
        else:
            # Need to take more step
            for prev_dot in res:
                new_val = self.dot_reachable(chain_id, target_dot, prev_dot)
                if new_val:
                    self.reachability_cache[(chain_id, target_dot)][block_dot] = True
                    return True
            self.reachability_cache[(chain_id, target_dot)][block_dot] = False
            return False

    def update_risk(self, chain_id: bytes, conf_peer_id: bytes, target_seq_num: int):
        print("Risk update: ", shorten(conf_peer_id), target_seq_num)
        self.peer_conf[(chain_id, target_seq_num)][conf_peer_id] += 1

    # ----------- Witness transactions --------------

    def schedule_witness_block(
        self, chain_id: bytes, seq_num: int, delay: float = None
    ):
        # Schedule witness transaction
        name_prefix = str(hex_to_int(chain_id + bytes(seq_num)))
        if self.is_pending_task_active(name_prefix):
            self.replace_task(
                name_prefix,
                self.witness,
                chain_id,
                seq_num,
                delay=self.settings.witness_delta_time,
            )
        else:
            self.register_task(
                name_prefix,
                self.witness,
                chain_id,
                seq_num,
                delay=self.settings.witness_delta_time,
            )

    def witness_tx_well_formatted(self, witness_tx: Any) -> bool:
        return len(witness_tx) == 2 and witness_tx[0] > 0 and len(witness_tx[1]) > 0

    def build_witness_blob(self, chain_id: bytes, seq_num: int) -> Optional[bytes]:
        chain_state = self.state_db.get_closest_peers_status(chain_id, seq_num)
        if not chain_state:
            return None
        return encode_raw(chain_state)

    def apply_witness_tx(
        self, block: BamiBlock, witness_tx: Tuple[int, ChainState]
    ) -> None:
        state = witness_tx[1]
        state_hash = take_hash(state)
        seq_num = witness_tx[0]

        if not self.should_witness_chain_point(block.com_id, block.public_key, seq_num):
            # This is invalid witnessing - react
            raise InvalidWitnessTransactionException(
                "Received invalid witness transaction",
                block.com_id,
                block.public_key,
                seq_num,
            )
        self.state_db.add_witness_vote(
            block.com_id, seq_num, state_hash, block.public_key
        )
        self.state_db.add_chain_state(block.com_id, seq_num, state_hash, state)

        chain_id = block.com_id
        if self.tracked_blocks.get(chain_id):
            for block_dot, tracked_block in self.tracked_blocks[chain_id].items():
                if (
                    block_dot[0] <= seq_num
                    and state.get(shorten(tracked_block.public_key))
                    and state.get(shorten(tracked_block.public_key)) == (True, True)
                ):
                    self.update_risk(chain_id, block.public_key, block_dot[0])

    # ------ Confirm and reject transactions -------

    def apply_confirm_tx(self, block: BamiBlock, confirm_tx: Dict) -> None:
        claim_dot = block.com_dot
        chain_id = block.com_id
        claimer = block.public_key
        com_links = block.links
        seq_num = claim_dot[0]
        self.state_db.apply_confirm(
            chain_id,
            claimer,
            com_links,
            claim_dot,
            confirm_tx[b"initiator"],
            confirm_tx[b"dot"],
            Decimal(confirm_tx[b"value"], self.context),
            self.should_store_store_update(chain_id, seq_num),
        )

    def apply_reject_tx(self, block: BamiBlock, reject_tx: Dict) -> None:
        self.state_db.apply_reject(
            block.com_id,
            block.public_key,
            block.links,
            block.com_dot,
            reject_tx[b"initiator"],
            reject_tx[b"dot"],
            self.should_store_store_update(block.com_id, block.com_seq_num),
        )

    async def unload(self):
        if not self.block_sign_queue_task.done():
            self.block_sign_queue_task.cancel()
        await super().unload()


# class PaymentIPv8Community(
#    IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy, PaymentCommunity
# ):
#    pass
