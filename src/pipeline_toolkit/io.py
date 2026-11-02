"""Streaming readers and writers for CSV, JSON and JSONL files.

Readers are generators: rows flow through the pipeline one at a time, so a
multi-gigabyte extract never has to fit in memory. Writers accept any iterable
of dicts, including generators, for the same reason.

Every reader yields ``dict[str, str | None]`` for CSV (values arrive as text —
coercion is the schema layer's job) and native JSON types for JSON/JSONL.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

Record = Mapping[str, Any]


def read_csv(path: str | Path, *, delimiter: str = ",") -> Iterator[dict[str, str | None]]:
    """Yield rows from a CSV file as dicts keyed by the header row."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        if reader.fieldnames is None:
            return
        for row in reader:
            # DictReader puts surplus columns under the None key; drop them so
            # downstream validation sees a clean record shape.
            row.pop(None, None)
            yield row


def read_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield records from a JSON Lines file (one JSON object per line)."""
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"{path}:{line_no}: expected a JSON object, got {type(obj)}")
            yield obj


def read_json(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield records from a JSON file containing a top-level array of objects."""
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError(f"{path}: expected a top-level JSON array")
    for i, obj in enumerate(data):
        if not isinstance(obj, dict):
            raise ValueError(f"{path}[{i}]: expected a JSON object, got {type(obj)}")
        yield obj


def read_records(path: str | Path) -> Iterator[dict[str, Any]]:
    """Dispatch to the right reader based on the file extension."""
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        yield from read_csv(path)
    elif suffix in (".jsonl", ".ndjson"):
        yield from read_jsonl(path)
    elif suffix == ".json":
        yield from read_json(path)
    else:
        raise ValueError(f"unsupported file extension: {suffix!r} ({path})")


def write_csv(path: str | Path, records: Iterable[Record], fieldnames: list[str]) -> int:
    """Write records to CSV. Returns the number of data rows written."""
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: record.get(k) for k in fieldnames})
            count += 1
    return count


def write_jsonl(path: str | Path, records: Iterable[Record]) -> int:
    """Write records as JSON Lines. Returns the number of rows written."""
    count = 0
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, default=str) + "\n")
            count += 1
    return count
