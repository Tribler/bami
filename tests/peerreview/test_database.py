from hashlib import sha256

from ipv8.keyvault.crypto import default_eccrypto

from bami.peerreview.database import TamperEvidentLog
from bami.peerreview.payload import TransactionPayload
from bami.peerreview.utils import get_random_string, payload_hash


def test_database_add_tx():
    db = TamperEvidentLog()

    p = default_eccrypto.generate_key(u'medium')
    p_id = p.key_to_bin()

    s = get_random_string(20)
    tx1 = TransactionPayload(s.encode())
    db.add_entry(p_id, 1, b'0', tx1)
    assert db.get_last_seq_num(p_id) == 1
    assert db.get_entry(p_id, 1).script == s.encode()


def test_database_add_tx_in_order():
    db = TamperEvidentLog()

    p = default_eccrypto.generate_key(u'medium')
    p_id = p.key_to_bin()

    s = get_random_string(20)
    tx1 = TransactionPayload(s.encode())

    s2 = get_random_string(20)
    tx2 = TransactionPayload(s2.encode())
    tx1_hash = payload_hash(tx1)

    db.add_entry(p_id, 1, b'0', tx1)
    assert db.get_last_seq_num(p_id) == 1
    assert db.get_entry(p_id, 1).script == s.encode()

    assert db.log_hashes[p_id].get(1) is not None
    assert db.log_hashes[p_id].get(2) is None

    prev_hash = sha256(b'0' + tx1_hash).digest()
    db.add_entry(p_id, 2, prev_hash, tx2)
    assert db.get_last_seq_num(p_id) == 2
    assert db.log_hashes[p_id].get(1) is not None
    assert db.log_hashes[p_id].get(2) is not None


def test_database_add_tx_out_order():
    db = TamperEvidentLog()

    p = default_eccrypto.generate_key(u'medium')
    p_id = p.key_to_bin()

    s = get_random_string(20)
    tx1 = TransactionPayload(s.encode())

    s2 = get_random_string(20)
    tx2 = TransactionPayload(s2.encode())
    tx1_hash = payload_hash(tx1)
    prev_hash = sha256(b'0' + tx1_hash).digest()

    db.add_entry(p_id, 2, prev_hash, tx2)
    assert db.get_last_seq_num(p_id) == 2
    assert db.get_entry(p_id, 2).script == s2.encode()

    assert db.log_hashes[p_id].get(1) is None
    assert db.log_hashes[p_id].get(2) is None

    db.add_entry(p_id, 1, b'0', tx1)
    assert db.get_last_seq_num(p_id) == 2
    assert db.log_hashes[p_id].get(1) is not None
    assert db.log_hashes[p_id].get(2) is not None
