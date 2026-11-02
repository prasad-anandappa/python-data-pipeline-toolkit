import json

from pipeline_toolkit import cli


def test_cli_validate_ok_and_bad(tmp_path, capsys):
    schema = tmp_path / "s.json"
    schema.write_text(json.dumps({"fields": [{"name": "n", "type": "int", "min_value": 0}]}))
    good = tmp_path / "good.csv"
    good.write_text("n\n1\n2\n")
    bad = tmp_path / "bad.csv"
    bad.write_text("n\n-5\n")
    assert cli.main(["validate", str(good), "--schema", str(schema)]) == 0
    assert cli.main(["validate", str(bad), "--schema", str(schema)]) == 1


def test_cli_convert_csv_to_jsonl(tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("id,name\n1,x\n")
    dest = tmp_path / "a.jsonl"
    assert cli.main(["convert", str(src), str(dest)]) == 0
    assert json.loads(dest.read_text()) == {"id": "1", "name": "x"}


def test_cli_dedup(tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("id,v\n1,old\n1,new\n")
    dest = tmp_path / "out.jsonl"
    assert cli.main(["dedup", str(src), "--key", "id", "-o", str(dest)]) == 0
    assert json.loads(dest.read_text()) == {"id": "1", "v": "new"}


def test_cli_quality_gate_exit_code(tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("id\n1\n1\n")
    assert cli.main(["quality", str(src), "--unique", "id"]) == 1
    assert cli.main(["quality", str(src)]) == 0
