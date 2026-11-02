import pytest

from pipeline_toolkit import Pipeline, QualityEngine, Schema
from pipeline_toolkit.loaders import SQLiteLoader
from pipeline_toolkit.pipeline import QualityGateError
from pipeline_toolkit.quality import NotNullRule, UniqueRule
from pipeline_toolkit.schema import Field

CSV = """payment_id,amount,status
pay_1,10.00,settled
pay_2,-1.00,settled
pay_1,12.00,settled
pay_3,7.50,pending
"""


@pytest.fixture()
def source(tmp_path):
    p = tmp_path / "payments.csv"
    p.write_text(CSV)
    return p


def make_pipeline(engine=None) -> Pipeline:
    return Pipeline(
        schema=Schema(
            [Field("payment_id", "str"), Field("amount", "decimal", min_value=0),
             Field("status", "str")]
        ),
        dedup_key="payment_id",
        quality_engine=engine,
    )


def test_pipeline_quarantines_bad_and_dedups(source, tmp_path):
    with SQLiteLoader(tmp_path / "t.db", "payments") as loader:
        report = make_pipeline().run(
            source, loader, batch_key="b1", key_columns=["payment_id"]
        )
        assert report.read == 4
        assert report.quarantined == 1  # the -1.00 row
        assert report.duplicates_removed == 1  # pay_1 twice -> latest wins
        assert loader.count() == 2


def test_pipeline_rerun_is_idempotent(source, tmp_path):
    with SQLiteLoader(tmp_path / "t.db", "payments") as loader:
        pipeline = make_pipeline()
        pipeline.run(source, loader, batch_key="b1", key_columns=["payment_id"])
        report = pipeline.run(source, loader, batch_key="b1", key_columns=["payment_id"])
        assert report.load is not None and report.load.status == "skipped_duplicate"
        assert loader.count() == 2


def test_quality_gate_blocks_load(source, tmp_path):
    engine = QualityEngine([UniqueRule("payment_id"), NotNullRule("amount")],
                           include_plugins=False)
    # unique passes post-dedup; now use a rule that fails on this data instead
    from pipeline_toolkit.quality import InSetRule

    engine = QualityEngine([InSetRule("status", {"settled"})], include_plugins=False)
    with SQLiteLoader(tmp_path / "t.db", "payments") as loader:
        with pytest.raises(QualityGateError):
            make_pipeline(engine).run(
                source, loader, batch_key="b1", key_columns=["payment_id"]
            )
        assert loader.count() == 0  # nothing was loaded
