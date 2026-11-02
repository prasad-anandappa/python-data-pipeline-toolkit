"""Record transforms: deduplication and normalisation.

Transforms are pure functions over ``list[dict]`` (or, for :func:`dedup`,
any iterable). They never mutate their inputs — pipelines stay easy to
reason about and safe to re-run.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

Record = dict[str, Any]


def dedup(
    records: Iterable[Record],
    *,
    key: str | tuple[str, ...] | Callable[[Record], Any],
    keep: str = "last",
) -> list[Record]:
    """Remove duplicate records by ``key``.

    Args:
        key: A column name, a tuple of column names (composite key), or a
            callable that computes the key from a record.
        keep: ``"last"`` (default — the freshest version of a record wins,
            which is the right default for CDC-style extracts where later
            rows supersede earlier ones) or ``"first"``.

    Output order is stable: records appear in the order of their *kept*
    occurrence.
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    if callable(key):
        key_fn = key
    elif isinstance(key, tuple):
        key_fn = lambda r: tuple(r.get(k) for k in key)  # noqa: E731
    else:
        key_fn = lambda r: r.get(key)  # noqa: E731

    if keep == "first":
        seen: set[Any] = set()
        out: list[Record] = []
        for record in records:
            k = key_fn(record)
            if k in seen:
                continue
            seen.add(k)
            out.append(record)
        return out

    # keep == "last": remember the last index for each key, then emit in
    # index order so the result follows source order.
    last_index: dict[Any, int] = {}
    materialised = list(records)
    for i, record in enumerate(materialised):
        last_index[key_fn(record)] = i
    keep_indexes = set(last_index.values())
    return [r for i, r in enumerate(materialised) if i in keep_indexes]


def rename(records: Iterable[Record], mapping: dict[str, str]) -> list[Record]:
    """Rename columns according to ``mapping`` (old name -> new name)."""
    return [{mapping.get(k, k): v for k, v in r.items()} for r in records]


def select(records: Iterable[Record], columns: list[str]) -> list[Record]:
    """Project records down to ``columns`` (missing columns become None)."""
    return [{c: r.get(c) for c in columns} for r in records]


def strip_strings(records: Iterable[Record]) -> list[Record]:
    """Trim leading/trailing whitespace from every string value."""
    return [
        {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        for r in records
    ]


def null_if_empty(records: Iterable[Record]) -> list[Record]:
    """Normalise empty strings to ``None`` (a common CSV artefact)."""
    return [
        {k: (None if isinstance(v, str) and v == "" else v) for k, v in r.items()}
        for r in records
    ]
