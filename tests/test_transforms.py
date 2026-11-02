import pytest

from pipeline_toolkit import transforms

ROWS = [
    {"id": 1, "v": "old"},
    {"id": 2, "v": "b"},
    {"id": 1, "v": "new"},
]


def test_dedup_keep_last_by_default():
    assert transforms.dedup(ROWS, key="id") == [{"id": 2, "v": "b"}, {"id": 1, "v": "new"}]


def test_dedup_keep_first():
    assert transforms.dedup(ROWS, key="id", keep="first") == [
        {"id": 1, "v": "old"},
        {"id": 2, "v": "b"},
    ]


def test_dedup_composite_key():
    rows = [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1, "x": 9}]
    assert transforms.dedup(rows, key=("a", "b"), keep="first") == rows[:2]


def test_dedup_callable_key():
    rows = [{"email": "A@x.io"}, {"email": "a@x.io"}]
    out = transforms.dedup(rows, key=lambda r: r["email"].lower(), keep="first")
    assert out == [{"email": "A@x.io"}]


def test_dedup_rejects_bad_keep():
    with pytest.raises(ValueError):
        transforms.dedup(ROWS, key="id", keep="random")


def test_dedup_does_not_mutate_input():
    rows = [dict(r) for r in ROWS]
    transforms.dedup(rows, key="id")
    assert rows == ROWS


def test_rename_select_strip_null():
    rows = [{" a ": " x ", "b": ""}]
    stripped = transforms.strip_strings(rows)
    assert stripped == [{" a ": "x", "b": ""}]
    assert transforms.null_if_empty(stripped) == [{" a ": "x", "b": None}]
    assert transforms.rename(rows, {"b": "c"}) == [{" a ": " x ", "c": ""}]
    assert transforms.select(rows, ["b"]) == [{"b": ""}]
