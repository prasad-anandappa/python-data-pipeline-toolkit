"""Data-quality rule engine with a plugin registry.

A *rule* is any object implementing the :class:`Rule` protocol: it receives
the materialised batch of records and returns a :class:`RuleResult`. Rules
declare a **severity**:

* ``ERROR`` — a failure fails the whole quality gate (the load is blocked).
* ``WARN``  — a failure is reported but the gate still passes; use this for
  checks that page a human rather than stop a pipeline.

**Extending the engine** — two ways:

1. In-process: decorate a rule class with :func:`register_rule`::

       @register_rule("no_negative_amounts")
       class NoNegativeAmounts(RowRule): ...

2. As a plugin package: expose rule classes through the
   ``pipeline_toolkit.rules`` entry-point group in your distribution's
   ``pyproject.toml`` and call :func:`load_plugin_rules` (the engine does
   this automatically unless ``include_plugins=False``).

   .. code-block:: toml

       [project.entry-points."pipeline_toolkit.rules"]
       my_rule = "my_package.rules:MyRule"
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from importlib import metadata
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

Record = dict[str, Any]
Severity = str  # "ERROR" | "WARN"

MAX_SAMPLES = 5


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class RuleResult:
    """Outcome of one rule over one batch."""

    rule: str
    severity: Severity
    passed: bool
    checked: int
    failed: int = 0
    samples: list[Any] = field(default_factory=list)  # offending values/rows (capped)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "severity": self.severity,
            "passed": self.passed,
            "checked": self.checked,
            "failed": self.failed,
            "samples": [str(s) for s in self.samples],
            "message": self.message,
        }


@dataclass
class QualityReport:
    """Aggregate result of running a :class:`QualityEngine` over a batch."""

    results: list[RuleResult]
    total_records: int

    @property
    def passed(self) -> bool:
        """The gate passes iff no ERROR-severity rule failed."""
        return all(r.passed for r in self.results if r.severity == "ERROR")

    @property
    def warnings(self) -> list[RuleResult]:
        return [r for r in self.results if r.severity == "WARN" and not r.passed]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "total_records": self.total_records,
            "results": [r.to_dict() for r in self.results],
        }

    def summary(self) -> str:
        lines = [
            f"quality gate: {'PASS' if self.passed else 'FAIL'} "
            f"({self.total_records} records, {len(self.results)} rules)"
        ]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(
                f"  [{status}] {r.rule} ({r.severity}) — "
                f"{r.failed}/{r.checked} failed"
                + (f" — {r.message}" if r.message else "")
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rule protocol + base classes
# ---------------------------------------------------------------------------


@runtime_checkable
class Rule(Protocol):
    """Protocol every rule (built-in, registered, or plugin) must satisfy."""

    name: str
    severity: Severity

    def check(self, records: list[Record]) -> RuleResult: ...


class RowRule(ABC):
    """Base class for rules that judge one row at a time.

    Subclasses implement :meth:`violation`, returning ``None`` for a good row
    or a short description of the problem. The base class handles counting
    and failure sampling.
    """

    name: str = "row_rule"
    severity: Severity = "ERROR"

    @abstractmethod
    def violation(self, record: Record) -> str | None:
        """Return a problem description, or None if the row is fine."""

    def check(self, records: list[Record]) -> RuleResult:
        failed = 0
        samples: list[Any] = []
        for record in records:
            problem = self.violation(record)
            if problem is not None:
                failed += 1
                if len(samples) < MAX_SAMPLES:
                    samples.append(f"{problem} :: {record}")
        return RuleResult(
            rule=self.name,
            severity=self.severity,
            passed=failed == 0,
            checked=len(records),
            failed=failed,
            samples=samples,
        )


class BatchRule(ABC):
    """Base class for rules that judge the batch as a whole (counts, sets)."""

    name: str = "batch_rule"
    severity: Severity = "ERROR"

    @abstractmethod
    def check(self, records: list[Record]) -> RuleResult: ...


# ---------------------------------------------------------------------------
# Registry + plugin loading
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, type] = {}


def register_rule(name: str) -> Callable[[type], type]:
    """Class decorator registering a rule class under ``name``."""

    def decorator(cls: type) -> type:
        if name in _REGISTRY:
            raise ValueError(f"rule name already registered: {name!r}")
        _REGISTRY[name] = cls
        cls.name = name  # type: ignore[attr-defined]
        return cls

    return decorator


def registered_rules() -> dict[str, type]:
    """Return a copy of the in-process rule registry."""
    return dict(_REGISTRY)


def load_plugin_rules(group: str = "pipeline_toolkit.rules") -> dict[str, type]:
    """Discover rule classes published via entry points.

    Returns ``{entry_point_name: class}``. Broken plugins raise — silently
    swallowing a failed quality rule would defeat the purpose of the gate.
    """
    discovered: dict[str, type] = {}
    for ep in metadata.entry_points(group=group):
        cls = ep.load()
        discovered[ep.name] = cls
        _REGISTRY.setdefault(ep.name, cls)
    return discovered


# ---------------------------------------------------------------------------
# Built-in rules
# ---------------------------------------------------------------------------


@register_rule("not_null")
class NotNullRule(RowRule):
    """Every value in ``column`` must be present and non-null."""

    def __init__(self, column: str, severity: Severity = "ERROR") -> None:
        self.column = column
        self.severity = severity
        self.name = f"not_null({column})"

    def violation(self, record: Record) -> str | None:
        if record.get(self.column) is None:
            return f"{self.column} is null"
        return None


@register_rule("unique")
class UniqueRule(BatchRule):
    """The combination of ``columns`` must be unique across the batch."""

    def __init__(self, *columns: str, severity: Severity = "ERROR") -> None:
        if not columns:
            raise ValueError("UniqueRule needs at least one column")
        self.columns = columns
        self.severity = severity
        self.name = f"unique({', '.join(columns)})"

    def check(self, records: list[Record]) -> RuleResult:
        seen: set[tuple[Any, ...]] = set()
        dupes: list[Any] = []
        for record in records:
            key = tuple(record.get(c) for c in self.columns)
            if key in seen and len(dupes) < MAX_SAMPLES:
                dupes.append(key)
            seen.add(key)
        failed = len(records) - len(seen)
        return RuleResult(
            rule=self.name,
            severity=self.severity,
            passed=failed == 0,
            checked=len(records),
            failed=failed,
            samples=dupes,
            message=f"{failed} duplicate key(s)" if failed else "",
        )


@register_rule("range")
class RangeRule(RowRule):
    """Numeric ``column`` values must fall within [min_value, max_value]."""

    def __init__(
        self,
        column: str,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
        severity: Severity = "ERROR",
    ) -> None:
        if min_value is None and max_value is None:
            raise ValueError("RangeRule needs at least one bound")
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.severity = severity
        self.name = f"range({column})"

    def violation(self, record: Record) -> str | None:
        value = record.get(self.column)
        if value is None:
            return None  # null-ness is NotNullRule's job
        try:
            numeric = float(value)  # value is Any-typed from the record dict
        except (TypeError, ValueError):
            return f"{self.column}={value!r} is not numeric"
        if self.min_value is not None and numeric < self.min_value:
            return f"{self.column}={numeric} < {self.min_value}"
        if self.max_value is not None and numeric > self.max_value:
            return f"{self.column}={numeric} > {self.max_value}"
        return None


@register_rule("regex")
class RegexRule(RowRule):
    """String values in ``column`` must fully match ``pattern``."""

    def __init__(self, column: str, pattern: str, severity: Severity = "ERROR") -> None:
        self.column = column
        self.pattern = re.compile(pattern)
        self.severity = severity
        self.name = f"regex({column})"

    def violation(self, record: Record) -> str | None:
        value = record.get(self.column)
        if value is None:
            return None
        if not self.pattern.fullmatch(str(value)):
            return f"{self.column}={value!r} does not match {self.pattern.pattern!r}"
        return None


@register_rule("in_set")
class InSetRule(RowRule):
    """Values in ``column`` must belong to a closed set of allowed values."""

    def __init__(self, column: str, allowed: Iterable[Any], severity: Severity = "ERROR") -> None:
        self.column = column
        self.allowed = set(allowed)
        self.severity = severity
        self.name = f"in_set({column})"

    def violation(self, record: Record) -> str | None:
        value = record.get(self.column)
        if value is not None and value not in self.allowed:
            return f"{self.column}={value!r} not in allowed set"
        return None


@register_rule("row_count")
class RowCountRule(BatchRule):
    """Batch size must fall within [min_rows, max_rows] — catches empty or
    truncated extracts, the most common silent pipeline failure."""

    def __init__(
        self,
        *,
        min_rows: int = 0,
        max_rows: int | None = None,
        severity: Severity = "ERROR",
    ) -> None:
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.severity = severity
        self.name = "row_count"

    def check(self, records: list[Record]) -> RuleResult:
        n = len(records)
        ok = n >= self.min_rows and (self.max_rows is None or n <= self.max_rows)
        message = "" if ok else f"row count {n} outside [{self.min_rows}, {self.max_rows}]"
        return RuleResult(
            rule=self.name,
            severity=self.severity,
            passed=ok,
            checked=n,
            failed=0 if ok else 1,
            message=message,
        )


@register_rule("references")
class ReferencedValuesRule(RowRule):
    """Foreign-key style check: values in ``column`` must exist in a
    reference set (e.g. every payment's account_id must be a known account)."""

    def __init__(self, column: str, reference: Iterable[Any], severity: Severity = "ERROR") -> None:
        self.column = column
        self.reference = set(reference)
        self.severity = severity
        self.name = f"references({column})"

    def violation(self, record: Record) -> str | None:
        value = record.get(self.column)
        if value is not None and value not in self.reference:
            return f"{self.column}={value!r} not found in reference set"
        return None


@register_rule("freshness")
class FreshnessRule(BatchRule):
    """The newest value in a date/datetime ``column`` must be no older than
    ``max_age`` — catches upstream jobs that silently stopped delivering."""

    def __init__(
        self,
        column: str,
        *,
        max_age: timedelta,
        now: datetime | None = None,
        severity: Severity = "ERROR",
    ) -> None:
        self.column = column
        self.max_age = max_age
        self.now = now or datetime.now(timezone.utc)
        self.severity = severity
        self.name = f"freshness({column})"

    def check(self, records: list[Record]) -> RuleResult:
        values: list[datetime] = []
        for record in records:
            raw = record.get(self.column)
            if isinstance(raw, datetime):
                values.append(raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc))
            elif isinstance(raw, date):
                values.append(datetime(raw.year, raw.month, raw.day, tzinfo=timezone.utc))
        if not values:
            return RuleResult(
                rule=self.name, severity=self.severity, passed=False,
                checked=len(records), failed=1,
                message=f"no parseable {self.column} values",
            )
        newest = max(values)
        age = self.now - newest
        ok = age <= self.max_age
        return RuleResult(
            rule=self.name, severity=self.severity, passed=ok,
            checked=len(records), failed=0 if ok else 1,
            message="" if ok else f"newest {self.column} is {age} old (limit {self.max_age})",
        )


@register_rule("custom")
class CustomRule(RowRule):
    """Adapt any ``record -> bool`` predicate into a rule."""

    def __init__(
        self,
        name: str,
        predicate: Callable[[Record], bool],
        severity: Severity = "ERROR",
    ) -> None:
        self.name = f"custom({name})"
        self.predicate = predicate
        self.severity = severity

    def violation(self, record: Record) -> str | None:
        return None if self.predicate(record) else "predicate returned False"


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class QualityEngine:
    """Run a set of rules over a batch and produce a :class:`QualityReport`."""

    def __init__(self, rules: Iterable[Rule], *, include_plugins: bool = True) -> None:
        if include_plugins:
            load_plugin_rules()
        self.rules = list(rules)

    def run(self, records: list[Record]) -> QualityReport:
        results = [rule.check(records) for rule in self.rules]
        return QualityReport(results=results, total_records=len(records))
