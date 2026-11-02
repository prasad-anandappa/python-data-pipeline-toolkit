import pytest

from pipeline_toolkit import io


def test_read_csv_yields_dicts(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("id,name\n1,alpha\n2,beta\n")
    assert list(io.read_csv(p)) == [
        {"id": "1", "name": "alpha"},
        {"id": "2", "name": "beta"},
    ]


def test_csv_roundtrip(tmp_path):
    p = tmp_path / "out.csv"
    rows = [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]
    assert io.write_csv(p, rows, ["id", "name"]) == 2
    assert list(io.read_csv(p)) == [{"id": "1", "name": "x"}, {"id": "2", "name": "y"}]


def test_jsonl_roundtrip_and_blank_lines(tmp_path):
    p = tmp_path / "a.jsonl"
    assert io.write_jsonl(p, [{"a": 1}, {"a": 2}]) == 2
    p.write_text(p.read_text() + "\n")  # trailing blank line is tolerated
    assert list(io.read_jsonl(p)) == [{"a": 1}, {"a": 2}]


def test_jsonl_invalid_line_reports_line_number(tmp_path):
    p = tmp_path / "bad.jsonl"
    p.write_text('{"a": 1}\nnot json\n')
    with pytest.raises(ValueError, match=":2:"):
        list(io.read_jsonl(p))


def test_read_json_array(tmp_path):
    p = tmp_path / "a.json"
    p.write_text('[{"x": 1}, {"x": 2}]')
    assert list(io.read_json(p)) == [{"x": 1}, {"x": 2}]


def test_read_records_rejects_unknown_extension(tmp_path):
    p = tmp_path / "a.parquet"
    p.write_text("nope")
    with pytest.raises(ValueError, match="unsupported"):
        list(io.read_records(p))
