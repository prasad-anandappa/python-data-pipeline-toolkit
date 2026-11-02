# python-data-pipeline-toolkit

[![CI](https://github.com/prasad-anandappa/python-data-pipeline-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/prasad-anandappa/python-data-pipeline-toolkit/actions/workflows/ci.yml)

A typed, dependency-free Python library for the four stages every batch data
pipeline needs — **ingest → validate → check → load** — built around one
conviction: pipelines fail in production not because the happy path is hard,
but because re-runs, partial files, and silent upstream changes are treated
as surprises. This toolkit makes them boring.

```python
from pipeline_toolkit import Pipeline, QualityEngine, Schema
from pipeline_toolkit.loaders import SQLiteLoader
from pipeline_toolkit.quality import NotNullRule, UniqueRule
from pipeline_toolkit.schema import Field

schema = Schema([
    Field("payment_id", "str", pattern=r"pay_[0-9]+"),
    Field("amount", "decimal", min_value=0),
    Field("paid_on", "date"),
])
engine = QualityEngine([UniqueRule("payment_id"), NotNullRule("amount")])
pipeline = Pipeline(schema=schema, dedup_key="payment_id", quality_engine=engine)

with SQLiteLoader("warehouse.db", table="payments") as loader:
    report = pipeline.run("payments.csv", loader,
                          batch_key="payments/2026-10-31",
                          key_columns=["payment_id"])
# Run it again: the load is a verified no-op. Run it with corrected data under
# the same batch key: a hard, loud error instead of silent corruption.
```

## What it does

| Stage | Module | Highlights |
|---|---|---|
| Ingest | `pipeline_toolkit.io` | Streaming CSV / JSON / JSONL readers (generators — constant memory), writers |
| Async ingest | `pipeline_toolkit.async_io` | Producer-thread → bounded-queue async reader with real backpressure; concurrent rule execution |
| Validate | `pipeline_toolkit.schema` | Declarative fields, type coercion (incl. `Decimal`, ISO dates), constraint checks, *all* errors reported at once, dead-letter partitioning |
| Transform | `pipeline_toolkit.transforms` | Dedup (first/last, composite & callable keys), rename/select/normalise — pure, non-mutating |
| Check | `pipeline_toolkit.quality` | Rule engine with severities, a class registry, and entry-point **plugins** |
| Load | `pipeline_toolkit.loaders` | Idempotent loads: batch ledger + content hashing + natural-key upserts; atomic file loads |
| Orchestrate | `pipeline_toolkit.pipeline` | Wires the stages with explicit failure semantics and a full run report |
| CLI | `pipeline_toolkit.cli` | `validate` / `quality` / `dedup` / `convert` subcommands for shell-driven workflows |

## Design decisions & trade-offs

**Idempotency is two mechanisms, not one.** A batch ledger (batch key +
SHA-256 of canonicalised contents) makes *re-running the same batch* a
verified no-op and *re-using a key for different data* a hard error. Natural
-key upserts handle the orthogonal case: *different* batches that overlap
(a late correction file) converge instead of duplicating. Either mechanism
alone leaves a hole; together they cover the retry/backfill/correction
triangle that dominates real pipeline incidents.

**Bad rows are quarantined, not fatal — and never silent.** Schema validation
partitions a batch into valid rows and `(row, errors)` dead letters. One
malformed record in a million-row extract should not block 999,999 good
payments; equally, dropping it quietly is how warehouses drift from source
systems. The run report counts and samples every quarantined row.

**Severity is a routing decision.** `ERROR` rules block the load; `WARN`
rules report and let it proceed. "Payment references an unknown account"
should page a human, not stop everyone else's settlements — severity is how
that judgement is encoded in the pipeline instead of in someone's memory.

**Standard library only.** The core has zero runtime dependencies. That is a
deliberate constraint: the coercion, hashing and rule logic here is not
better served by pandas, and a zero-dependency library drops into any
environment (Airflow workers, Lambda, a vendor's locked-down VM) without a
dependency negotiation. The cost is real too — no vectorised speed — and the
README is honest about it under *Scaling notes*.

**Async exists for integration, not speed.** `aiter_records` bridges a
producer thread into an `asyncio` consumer through a bounded queue, so the
toolkit can sit inside an async service without blocking its event loop.
For a plain batch script the sync API is simpler and equally fast; the async
path earns its complexity only when ingestion is one stage of a larger async
system.

## Failure modes this design addresses

- **Scheduler retry after a partial load** → batch ledger makes the retry a no-op.
- **Truncated upstream extract** → `RowCountRule` + schema gate fail loudly before load.
- **Upstream job silently stops** → `FreshnessRule` on the max event timestamp.
- **Late-arriving correction file** → natural-key upsert updates in place.
- **Crash mid-write to a file target** → temp-file + `os.replace` atomic rename.
- **Duplicate delivery in a CDC extract** → `dedup(keep="last")` supersession semantics.

Known limits: the SQLite loader infers column types from data (it teaches the
ledger/upsert pattern; production use would manage DDL explicitly), and the
batch is materialised in memory for quality checks — see below.

## Scaling notes

Readers stream, but the quality engine and dedup materialise the batch:
rules like `UniqueRule` are inherently batch-level. For extracts beyond
memory, the escape hatches are (a) chunk the input and run per-chunk rules
with a merge step for batch rules, or (b) push set-based checks (uniqueness,
references) into SQL at the target. The `Rule` protocol is intentionally
narrow (`list[Record] -> RuleResult`) so a SQL-backed rule can implement the
same interface against a staged table. Throughput on a single core is
bounded by pure-Python coercion — order of 100k rows/sec for typical
schemas; if that is your bottleneck, this library is the wrong layer and a
columnar engine (DuckDB/Polars) should own the transform step.

## Extending: custom rules & plugins

```python
from pipeline_toolkit.quality import RowRule, register_rule

@register_rule("non_negative_amount")
class NonNegativeAmount(RowRule):
    def violation(self, record):
        amount = record.get("amount")
        return "amount is negative" if amount is not None and amount < 0 else None
```

Third-party packages can publish rules without touching this codebase, via
the `pipeline_toolkit.rules` entry-point group — the engine discovers them
automatically (see the `quality` module docstring).

## Quickstart

```bash
pip install -e ".[dev]"
python -m pytest            # 60 tests, incl. hypothesis property tests
python -m mypy              # strict mode, clean
python examples/run_payments_pipeline.py

# CLI
pipeline-toolkit validate examples/payments.csv --schema examples/payments.schema.json
pipeline-toolkit quality examples/payments.csv --unique payment_id --not-null amount
pipeline-toolkit convert examples/payments.csv /tmp/payments.jsonl
```

## What I learned / next steps

- Canonical-JSON hashing for batch identity is easy to get subtly wrong
  (key order, Decimal serialisation) — the property tests exist because of
  two real bugs found there during development.
- Next: a DuckDB-backed loader implementing the same ledger protocol, chunked
  quality execution with mergeable batch-rule state, and JSON Schema export
  from `Schema` for cross-language contracts.

## License

MIT — see [LICENSE](LICENSE).
