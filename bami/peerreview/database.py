from collections import defaultdict
from enum import Enum
from hashlib import sha256
from typing import Any, Optional, Set

from ipv8.types import Payload

from bami.peerreview.payload import LogEntryPayload
from bami.peerreview.utils import payload_hash


class InconsistentLog(Exception):
    pass


class EntryType(Enum):
    SEND = True
    RECEIVE = False


class PeerTxDB:

    def __init__(self):
        self.peer_txs = defaultdict(lambda: set())
        self.tx_payloads = {}

    def add_tx_payload(self, tx_id: Any, tx_payload: Payload):
        self.tx_payloads[tx_id] = tx_payload

    def get_tx_payload(self, tx_id: Any) -> Optional[Payload]:
        return self.tx_payloads.get(tx_id, None)

    def add_peer_tx(self, p_id: Any, tx_id: Any):
        self.peer_txs[p_id].add(tx_id)

    def get_peer_txs(self, p_id: Any) -> Set[Any]:
        return self.peer_txs[p_id]


class TamperEvidentLog:

    def __init__(self):
        # PeerId -> Message Auth?

        self.peer_log = defaultdict(lambda: {})

        self.peer_txs = defaultdict(lambda: {})
        self.last_seq_num = defaultdict(int)

        self.entries = defaultdict(lambda: {})

        self.log_hashes = defaultdict(lambda: {})
        self.pending_hashes = defaultdict(lambda: {})

    # def add_entry(self, p_pk: Any, sn: int, is_send: bool, cp_pk: Any, cp_sn: int, m_id: bytes, prev_hash: bytes):
    #    self.peer_log[p_pk][sn] =

    def create_new_entry(self, p_id: bytes, entry_type: EntryType,
                         cp_id: bytes, cp_seq_num: int, message: bytes) -> LogEntryPayload:
        sn = self.get_last_seq_num(p_id)
        prev_hash = self.log_hashes[p_id].get(sn, None) if sn > 1 else b'0'

        new_entry = LogEntryPayload(p_id, sn + 1, entry_type, prev_hash, cp_id, cp_seq_num, message)
        self.entries[p_id][sn + 1] = new_entry

        self.log_hashes[p_id][sn + 1] = payload_hash(new_entry)
        return new_entry

    def add_entry(self, p_id: str, seq_num: int, claimed_prev_hash: Any, entry: Any):
        prev_hash = self.log_hashes[p_id].get(seq_num, None) if seq_num > 1 else b'0'
        if prev_hash and prev_hash != claimed_prev_hash:
            raise InconsistentLog("Peer {} has inconsistent log: \n got {}  \n vs expected {}".format(p_id,
                                                                                                      claimed_prev_hash,
                                                                                                      prev_hash))

        self.peer_txs[p_id][seq_num] = entry
        self.last_seq_num[p_id] = max(self.peer_txs[p_id])

        tx_hash = payload_hash(entry)
        if prev_hash:
            self.log_hashes[p_id][seq_num] = sha256(prev_hash + tx_hash).digest()

            # Check if there are pending hashes left
            c_i = seq_num
            pen_hash = self.pending_hashes[p_id].get(c_i + 1)
            while pen_hash:
                prev_hash = self.log_hashes[p_id].get(c_i)
                self.log_hashes[p_id][c_i + 1] = sha256(prev_hash + pen_hash).digest()

                c_i += 1
                pen_hash = self.pending_hashes[p_id].get(c_i + 1)
                del self.pending_hashes[p_id][c_i]
        else:
            self.pending_hashes[p_id][seq_num] = tx_hash

    def get_last_seq_num(self, p_id: bytes) -> int:
        return self.last_seq_num[p_id]

    def get_entry(self, p_id: bytes, seq_num: int) -> Optional[Any]:
        return self.peer_txs[p_id].get(seq_num, None)
