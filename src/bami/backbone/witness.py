from abc import abstractmethod, ABCMeta
from typing import Any, Optional

from bami.backbone.block import BamiBlock
from bami.backbone.block_sync import BlockSyncMixin
from bami.backbone.exceptions import InvalidTransactionFormatException
from bami.backbone.utils import decode_raw, shorten, WITNESS_TYPE


class WitnessMixin(BlockSyncMixin, metaclass=ABCMeta):

    @abstractmethod
    def witness_tx_well_formatted(self, witness_tx: Any) -> bool:
        """
        Returns:
            False if bad format
        """
        pass

    @abstractmethod
    def build_witness_blob(self, chain_id: bytes, seq_num: int) -> Optional[bytes]:
        """
        Args:
            chain_id: bytes identifier of the chain
            seq_num: of the chain to audit to
        Returns:
            witness blob (bytes) if possible, None otherwise
        """
        pass

    @abstractmethod
    def apply_witness_tx(self, block: BamiBlock, witness_tx: Any) -> None:
        pass

    def verify_witness_transaction(self, chain_id: bytes, witness_tx: Any) -> None:
        """
        Verify the witness transaction
        Raises:
             InvalidFormatTransaction
        """
        # 1. Witness transaction ill-formatted
        if not self.witness_tx_well_formatted(witness_tx):
            raise InvalidTransactionFormatException(
                "Invalid witness transaction", chain_id, witness_tx
            )

    def witness(self, chain_id: bytes, seq_num: int) -> None:
        """
        Witness the chain up to a sequence number.
        If chain is known and data exists:
         - Will create a witness block, link to latest known blocks and share in the community.
        Otherwise:
         - Do nothing
        Args:
            chain_id: id of the chain
            seq_num: sequence number of the block:
        """
        chain = self.persistence.get_chain(chain_id)
        if chain:
            witness_blob = self.build_witness_blob(chain_id, seq_num)
            if witness_blob:
                blk = self.create_signed_block(
                    block_type=WITNESS_TYPE,
                    transaction=witness_blob,
                    prefix=b"w",
                    com_id=chain_id,
                    use_consistent_links=False,
                )
                self.logger.debug(
                    "Creating witness block on chain %s: %s, com_dot %s, pers_dot %s",
                    shorten(blk.com_id),
                    seq_num,
                    blk.com_dot,
                    blk.pers_dot,
                )
                self.share_in_community(blk, chain_id)

    def process_witness(self, block: BamiBlock) -> None:
        """Process received witness transaction"""
        witness_tx = self.unpack_witness_blob(block.transaction)
        chain_id = block.com_id
        self.verify_witness_transaction(chain_id, witness_tx)
        # Apply to db
        self.apply_witness_tx(block, witness_tx)

    def unpack_witness_blob(self, witness_blob: bytes) -> Any:
        """
        Returns:
            decoded witness transaction
        """
        return decode_raw(witness_blob)
