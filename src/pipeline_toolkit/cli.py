"""Command-line interface — a first-class entry point, not an afterthought.

Subcommands:

* ``validate``  — check a file against a JSON schema; prints every violation
  and exits non-zero when any record is invalid.
* ``quality``   — run built-in quality checks (row count, uniqueness,
  not-null) and print a gate report.
* ``dedup``     — write a de-duplicated copy of a file.
* ``convert``   — convert between CSV / JSON / JSONL.

Examples::

    pipeline-toolkit validate payments.csv --schema payments.schema.json
    pipeline-toolkit quality payments.csv --unique payment_id --not-null amount
    pipeline-toolkit dedup payments.csv --key payment_id -o payments.clean.csv
    pipeline-toolkit convert payments.csv payments.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pipeline_toolkit import io as io_module
from pipeline_toolkit import transforms
from pipeline_toolkit.quality import (
    NotNullRule,
    QualityEngine,
    RowCountRule,
    UniqueRule,
)
from pipeline_toolkit.schema import Schema, SchemaError


def _load_schema(path: str) -> Schema:
    return Schema.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def cmd_validate(args: argparse.Namespace) -> int:
    schema = _load_schema(args.schema)
    records = list(io_module.read_records(args.file))
    valid, invalid = schema.partition(records)
    print(f"{len(valid)} valid, {len(invalid)} invalid of {len(records)} records")
    for record, err in invalid:
        print(f"  INVALID {record}\n    {err}")
    return 1 if invalid else 0


def cmd_quality(args: argparse.Namespace) -> int:
    records = list(io_module.read_records(args.file))
    rules: list[Any] = [RowCountRule(min_rows=args.min_rows)]
    if args.unique:
        rules.append(UniqueRule(*args.unique))
    for column in args.not_null or []:
        rules.append(NotNullRule(column))
    report = QualityEngine(rules, include_plugins=False).run(records)
    print(report.summary())
    return 0 if report.passed else 1


def cmd_dedup(args: argparse.Namespace) -> int:
    records = list(io_module.read_records(args.file))
    out = transforms.dedup(records, key=tuple(args.key) if len(args.key) > 1 else args.key[0],
                            keep=args.keep)
    removed = len(records) - len(out)
    suffix = Path(args.output).suffix.lower()
    if suffix == ".csv":
        fieldnames = list(out[0].keys()) if out else []
        io_module.write_csv(args.output, out, fieldnames)
    else:
        io_module.write_jsonl(args.output, out)
    print(f"{len(records)} -> {len(out)} records ({removed} duplicates removed)")
    return 0


def cmd_convert(args: argparse.Namespace) -> int:
    records = list(io_module.read_records(args.source))
    suffix = Path(args.dest).suffix.lower()
    if suffix == ".csv":
        fieldnames = list(records[0].keys()) if records else []
        io_module.write_csv(args.dest, records, fieldnames)
    elif suffix in (".jsonl", ".ndjson"):
        io_module.write_jsonl(args.dest, records)
    elif suffix == ".json":
        Path(args.dest).write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
    else:
        print(f"unsupported destination extension: {suffix}", file=sys.stderr)
        return 2
    print(f"wrote {len(records)} records to {args.dest}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-toolkit",
        description="Data-pipeline utilities: validate, check, dedup, convert.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate", help="validate a file against a JSON schema")
    p.add_argument("file")
    p.add_argument("--schema", required=True)
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("quality", help="run built-in data-quality checks")
    p.add_argument("file")
    p.add_argument("--unique", nargs="+", metavar="COL", help="columns that must be unique")
    p.add_argument("--not-null", nargs="+", metavar="COL", help="columns that must be non-null")
    p.add_argument("--min-rows", type=int, default=1)
    p.set_defaults(func=cmd_quality)

    p = sub.add_parser("dedup", help="write a de-duplicated copy of a file")
    p.add_argument("file")
    p.add_argument("--key", nargs="+", required=True, metavar="COL")
    p.add_argument("--keep", choices=["first", "last"], default="last")
    p.add_argument("-o", "--output", required=True)
    p.set_defaults(func=cmd_dedup)

    p = sub.add_parser("convert", help="convert between CSV / JSON / JSONL")
    p.add_argument("source")
    p.add_argument("dest")
    p.set_defaults(func=cmd_convert)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except SchemaError as exc:
        print(f"schema error: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
