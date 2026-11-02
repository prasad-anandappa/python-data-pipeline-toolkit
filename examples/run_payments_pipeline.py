"""End-to-end demo: load a payments extract into SQLite, safely, twice.

Run from the repo root:

    pip install -e .
    python examples/run_payments_pipeline.py

The sample extract deliberately contains the four defects that break real
pipelines: a duplicate row (pay_1003 appears twice — the later, settled
version must win), a missing amount (pay_1007), a negative amount
(pay_1009), and a payment for an unknown account (pay_1008 -> acc_999).
The second run proves idempotency: same batch key + same contents = no-op.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pipeline_toolkit import Pipeline, QualityEngine, Schema
from pipeline_toolkit.loaders import SQLiteLoader
from pipeline_toolkit.quality import NotNullRule, ReferencedValuesRule, RowCountRule, UniqueRule

HERE = Path(__file__).parent
KNOWN_ACCOUNTS = {"acc_501", "acc_502", "acc_503", "acc_504", "acc_505", "acc_506"}


def build_pipeline() -> Pipeline:
    schema = Schema.from_dict(
        __import__("json").loads((HERE / "payments.schema.json").read_text())
    )
    engine = QualityEngine(
        [
            RowCountRule(min_rows=1),
            UniqueRule("payment_id"),
            NotNullRule("amount"),
            # Unknown accounts page a human (WARN) instead of blocking the
            # load of everyone else's good payments.
            ReferencedValuesRule("account_id", KNOWN_ACCOUNTS, severity="WARN"),
        ],
        include_plugins=False,
    )
    return Pipeline(schema=schema, dedup_key="payment_id", quality_engine=engine)


def main() -> None:
    db = Path(tempfile.mkdtemp()) / "payments.db"
    pipeline = build_pipeline()
    with SQLiteLoader(db, table="payments") as loader:
        for run in (1, 2):
            report = pipeline.run(
                HERE / "payments.csv",
                loader,
                batch_key="payments/2026-10-31",
                key_columns=["payment_id"],
            )
            print(f"--- run {run} ---")
            print(f"read={report.read} valid={report.valid} "
                  f"quarantined={report.quarantined} "
                  f"duplicates_removed={report.duplicates_removed}")
            for err in report.quarantine_errors:
                print(f"  quarantined: {err}")
            assert report.quality is not None
            print(report.quality.summary())
            print(f"load: {report.load}")
            print(f"rows in table: {loader.count()}")


if __name__ == "__main__":
    main()
