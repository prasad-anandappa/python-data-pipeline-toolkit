import pytest

from pipeline_toolkit.loaders import AtomicFileLoader, LoadConflictError, SQLiteLoader

ROWS = [
    {"payment_id": "pay_1", "amount": 10.5, "status": "settled"},
    {"payment_id": "pay_2", "amount": 3.25, "status": "pending"},
]


@pytest.fixture()
def loader(tmp_path):
    with SQLiteLoader(tmp_path / "t.db", table="payments") as ld:
        yield ld


def test_load_then_rerun_is_noop(loader):
    first = loader.load(ROWS, batch_key="b1", key_columns=["payment_id"])
    second = loader.load(ROWS, batch_key="b1", key_columns=["payment_id"])
    assert first.status == "loaded" and first.rows_written == 2
    assert second.status == "skipped_duplicate"
    assert loader.count() == 2


def test_same_key_different_contents_is_conflict(loader):
    loader.load(ROWS, batch_key="b1", key_columns=["payment_id"])
    changed = [dict(ROWS[0], amount=11.0), ROWS[1]]
    with pytest.raises(LoadConflictError):
        loader.load(changed, batch_key="b1", key_columns=["payment_id"])


def test_later_batch_upserts_instead_of_duplicating(loader):
    loader.load(ROWS, batch_key="b1", key_columns=["payment_id"])
    correction = [{"payment_id": "pay_2", "amount": 3.25, "status": "settled"}]
    loader.load(correction, batch_key="b2", key_columns=["payment_id"])
    assert loader.count() == 2  # pay_2 updated in place, not duplicated


def test_empty_batch_is_recorded(loader):
    result = loader.load([], batch_key="empty", key_columns=["payment_id"])
    assert result.status == "loaded" and result.rows_written == 0
    again = loader.load([], batch_key="empty", key_columns=["payment_id"])
    assert again.status == "skipped_duplicate"


def test_atomic_file_loader(tmp_path):
    target = tmp_path / "out.jsonl"
    fl = AtomicFileLoader(target)
    assert fl.load(ROWS, batch_key="b1").status == "loaded"
    assert fl.load(ROWS, batch_key="b1").status == "skipped_duplicate"
    with pytest.raises(LoadConflictError):
        fl.load([{"payment_id": "pay_9"}], batch_key="b1")


def test_content_hash_ignores_key_order():
    from pipeline_toolkit.loaders import content_hash

    a = [{"x": 1, "y": 2}]
    b = [{"y": 2, "x": 1}]
    assert content_hash(a) == content_hash(b)
