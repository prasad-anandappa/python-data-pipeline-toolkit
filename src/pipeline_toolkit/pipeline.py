"""Pipeline orchestration: ingest -> validate -> dedup -> check -> load.

:class:`Pipeline` wires the library's pieces into one run with explicit
failure semantics:

* Records that fail schema validation are **quarantined** (dead-letter),
  counted, and reported — they never silently disappear and never poison
  the good rows.
* If the quality gate fails on an ERROR rule, the load **does not happen**
  and the run raises :class:`QualityGateError` carrying the report.
* Loads go through the idempotent loaders, so re-running the same pipeline
  invocation is always safe.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pipeline_toolkit import io as io_module
from pipeline_toolkit import transforms
from pipeline_toolkit.loaders import LoadResult, SQLiteLoader
from pipeline_toolkit.quality import QualityEngine, QualityReport
from pipeline_toolkit.schema import Schema, SchemaError

Record = dict[str, Any]


class QualityGateError(RuntimeError):
    """Raised when a batch fails an ERROR-severity quality rule."""

    def __init__(self, report: QualityReport) -> None:
        self.report = report
        super().__init__(f"quality gate failed:\n{report.summary()}")


@dataclass
class RunReport:
    """What happened in one pipeline run — the audit trail."""

    source: str
    batch_key: str
    read: int = 0
    valid: int = 0
    quarantined: int = 0
    quarantine_errors: list[str] = field(default_factory=list)
    duplicates_removed: int = 0
    quality: QualityReport | None = None
    load: LoadResult | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "batch_key": self.batch_key,
            "read": self.read,
            "valid": self.valid,
            "quarantined": self.quarantined,
            "quarantine_errors": self.quarantine_errors,
            "duplicates_removed": self.duplicates_removed,
            "quality": self.quality.to_dict() if self.quality else None,
            "load": vars(self.load) if self.load else None,
        }


class Pipeline:
    """A configurable batch pipeline over a single source file.

    Args:
        schema: Optional :class:`Schema`; when present, records are coerced
            and invalid ones quarantined.
        dedup_key: Optional column name / tuple / callable passed to
            :func:`pipeline_toolkit.transforms.dedup`.
        quality_engine: Optional :class:`QualityEngine` gating the load.
        transforms: Extra ``list -> list`` steps applied after validation,
            in order.
    """

    def __init__(
        self,
        *,
        schema: Schema | None = None,
        dedup_key: str | tuple[str, ...] | Callable[[Record], Any] | None = None,
        quality_engine: QualityEngine | None = None,
        transforms: list[Callable[[list[Record]], list[Record]]] | None = None,
    ) -> None:
        self.schema = schema
        self.dedup_key = dedup_key
        self.quality_engine = quality_engine
        self.transforms = transforms or []

    def run(
        self,
        source: str | Path,
        loader: SQLiteLoader,
        *,
        batch_key: str,
        key_columns: list[str],
    ) -> RunReport:
        """Execute the pipeline. See the module docstring for semantics."""
        report = RunReport(source=str(source), batch_key=batch_key)

        raw = list(io_module.read_records(source))
        report.read = len(raw)

        records = raw
        if self.schema is not None:
            valid, invalid = self.schema.partition(raw)
            records = valid
            report.quarantined = len(invalid)
            for _record, err in invalid[:10]:
                report.quarantine_errors.append(str(err))
        report.valid = len(records)

        if self.dedup_key is not None:
            deduped = transforms.dedup(records, key=self.dedup_key)
            report.duplicates_removed = len(records) - len(deduped)
            records = deduped

        for step in self.transforms:
            records = step(records)

        if self.quality_engine is not None:
            quality = self.quality_engine.run(records)
            report.quality = quality
            if not quality.passed:
                raise QualityGateError(quality)

        report.load = loader.load(records, batch_key=batch_key, key_columns=key_columns)
        return report
