import pytest
from python_project.backbone.datastore.block_store import LMDBLockStore


@pytest.fixture
def lmdb_store(tmpdir):
    tmp_val = tmpdir
    path = str(tmp_val)
    print(path)
    db = LMDBLockStore(path)
    yield db
    db.close()
    tmp_val.remove()


def test_add_block(lmdb_store):
    test_blob = b"123123123123123"
    test_key = b"lopo1"

    lmdb_store.add_block(test_key, test_blob)
    res = lmdb_store.get_block_by_hash(test_key)
    assert res == test_blob


def test_add_tx(lmdb_store):
    test_blob = b"123123123123123"
    test_key = b"lopo1"

    lmdb_store.add_tx(test_key, test_blob)
    res = lmdb_store.get_tx_by_hash(test_key)
    assert res == test_blob


def test_add_dot(lmdb_store):
    test_blob = b"123123123123123"
    test_key = b"lopo1"

    lmdb_store.add_dot(test_key, test_blob)
    res = lmdb_store.get_hash_by_dot(test_key)
    assert res == test_blob
