from abc import abstractmethod, ABCMeta
from asyncio import sleep, ensure_future, PriorityQueue
from collections import defaultdict
from enum import Enum
from typing import Dict

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.exceptions import InvalidTransactionFormatException
from bami.plexus.backbone.mixins import StatedMixin
from bami.plexus.backbone.utils import (
    decode_raw,
    EMPTY_PK,
    encode_raw,
    CONFIRM_TYPE,
    REJECT_TYPE,
)


class BlockResponse(Enum):
    CONFIRM = 1
    REJECT = 2
    DELAY = 3


class BlockResponseMixin(StatedMixin, metaclass=ABCMeta):
    """
    Adding this mixin class to your overlays enables routines to respond to incoming blocks with another block.
    """

    def setup_mixin(self) -> None:
        # Dictionary chain_id: block_dot -> block
        self.tracked_blocks = defaultdict(lambda: {})
        self.block_sign_queue_task = ensure_future(
            self.evaluate_counter_signing_blocks()
        )
        self.counter_signing_block_queue = PriorityQueue()

    def unload_mixin(self) -> None:
        if not self.block_sign_queue_task.done():
            self.block_sign_queue_task.cancel()

    @abstractmethod
    def block_response(
        self, block: PlexusBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        """
        Respond to block BlockResponse: Reject, Confirm, Delay
        Args:
            block: to respond to
            wait_time: time that passed since first block process initiated
            wait_blocks: number of blocks passed since the block
        Returns:
            BlockResponse: Confirm, Reject or Delay
        """
        pass

    def confirm_tx_extra_data(self, block: PlexusBlock) -> Dict:
        """
        Return additional data that should be added to the confirm transaction.
        Args:
            block: The block that is about to be confirmed.

        Returns: A dictionary with values to add to the confirm transaction.
        """
        return {}

    def add_block_to_response_processing(self, block: PlexusBlock) -> None:
        self.counter_signing_block_queue.put_nowait((block.com_seq_num, (0, block)))

    def process_counter_signing_block(
        self,
        block: PlexusBlock,
        time_passed: float = None,
        num_block_passed: int = None,
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
            self.confirm(block, extra_data=self.confirm_tx_extra_data(block))
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
                self.tracked_blocks[block.com_id].pop(block.com_dot, None)
                await sleep(0.001)

    def confirm(self, block: PlexusBlock, extra_data: Dict = None) -> None:
        """
        Confirm the transaction in an incoming block. Link will be in the transaction with block dot.
        Args:
            block: The PlexusBlock to confirm.
            extra_data: An optional dictionary with extra data that is appended to the confirmation.
        """
        self.logger.info("Confirming block %s", block)
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        confirm_tx = {b"initiator": block.public_key, b"dot": dot}
        if extra_data:
            confirm_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=CONFIRM_TYPE, transaction=encode_raw(confirm_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def reject(self, block: PlexusBlock, extra_data: Dict = None) -> None:
        """
        Reject the transaction in an incoming block.

        Args:
            block: The PlexusBlock to reject.
            extra_data: Some additional data to append to the reject transaction, e.g., a reason.
        """
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        reject_tx = {b"initiator": block.public_key, b"dot": dot}
        if extra_data:
            reject_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=REJECT_TYPE, transaction=encode_raw(reject_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def verify_confirm_tx(self, claimer: bytes, confirm_tx: Dict) -> None:
        # 1. verify claim format
        if not confirm_tx.get(b"initiator") or not confirm_tx.get(b"dot"):
            raise InvalidTransactionFormatException(
                "Invalid confirmation ", claimer, confirm_tx
            )

    def process_confirm(self, block: PlexusBlock) -> None:
        confirm_tx = decode_raw(block.transaction)
        self.verify_confirm_tx(block.public_key, confirm_tx)
        self.apply_confirm_tx(block, confirm_tx)

    @abstractmethod
    def apply_confirm_tx(self, block: PlexusBlock, confirm_tx: Dict) -> None:
        pass

    def verify_reject_tx(self, rejector: bytes, confirm_tx: Dict) -> None:
        # 1. verify reject format
        if not confirm_tx.get(b"initiator") or not confirm_tx.get(b"dot"):
            raise InvalidTransactionFormatException(
                "Invalid reject ", rejector, confirm_tx
            )

    def process_reject(self, block: PlexusBlock) -> None:
        reject_tx = decode_raw(block.transaction)
        self.verify_reject_tx(block.public_key, reject_tx)
        self.apply_reject_tx(block, reject_tx)

    @abstractmethod
    def apply_reject_tx(self, block: PlexusBlock, reject_tx: Dict) -> None:
        pass
