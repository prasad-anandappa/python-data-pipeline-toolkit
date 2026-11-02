import asyncio

from pipeline_toolkit.async_io import aiter_batches, aiter_records, run_rules_async
from pipeline_toolkit.quality import NotNullRule, UniqueRule


def test_aiter_records_reads_csv(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("id,v\n1,x\n2,y\n3,z\n")

    async def collect():
        return [r async for r in aiter_records(p)]

    rows = asyncio.run(collect())
    assert [r["id"] for r in rows] == ["1", "2", "3"]


def test_aiter_batches(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_text("\n".join('{"n": %d}' % i for i in range(7)))

    async def collect():
        return [b async for b in aiter_batches(p, batch_size=3)]

    batches = asyncio.run(collect())
    assert [len(b) for b in batches] == [3, 3, 1]


def test_run_rules_async_matches_sync_results(tmp_path):
    rows = [{"id": 1, "v": None}, {"id": 1, "v": "x"}]
    report = asyncio.run(run_rules_async([UniqueRule("id"), NotNullRule("v")], rows))
    assert not report.passed
    assert report.total_records == 2
