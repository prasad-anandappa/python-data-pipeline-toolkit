"""Async ingestion path.

File parsing is I/O + CPU work that does not belong on the event loop, so
the async reader uses a classic producer/consumer bridge: a worker thread
streams records through an :class:`asyncio.Queue` while the async consumer
processes them. Backpressure is real — the queue is bounded, so a fast disk
cannot outrun a slow consumer and blow up memory.

Use this path when ingestion is one stage of a larger async system (an
async API ingesting uploads, a streaming consumer, …). For plain batch
scripts the synchronous API in :mod:`pipeline_toolkit.io` is simpler and
just as fast; the async path exists for integration, not raw speed —
that trade-off is deliberate and documented.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Iterable
from pathlib import Path
from typing import Any

from pipeline_toolkit import io as sync_io
from pipeline_toolkit.quality import QualityReport, Rule, RuleResult

Record = dict[str, Any]

_SENTINEL = object()


async def aiter_records(
    path: str | Path, *, queue_size: int = 256
) -> AsyncIterator[Record]:
    """Asynchronously yield records from a CSV/JSON/JSONL file.

    A producer thread reads the file with the synchronous readers and feeds
    a bounded queue; this generator drains it. Exceptions raised in the
    reader thread are re-raised here, in order.
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
    error: list[BaseException] = []

    def producer() -> None:
        try:
            for record in sync_io.read_records(path):
                fut = asyncio.run_coroutine_threadsafe(queue.put(record), loop)
                fut.result()  # blocks when the queue is full -> backpressure
        except BaseException as exc:  # propagate to the consumer
            error.append(exc)
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(_SENTINEL), loop).result()

    thread = threading.Thread(target=producer, daemon=True, name="aiter-records")
    thread.start()
    try:
        while True:
            item = await queue.get()
            if item is _SENTINEL:
                break
            yield item
    finally:
        thread.join(timeout=5)
    if error:
        raise error[0]


async def aiter_batches(
    path: str | Path, *, batch_size: int = 500
) -> AsyncIterator[list[Record]]:
    """Yield records from ``path`` in lists of up to ``batch_size``."""
    batch: list[Record] = []
    async for record in aiter_records(path):
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


async def run_rules_async(rules: Iterable[Rule], records: list[Record]) -> QualityReport:
    """Run quality rules concurrently.

    Rules are CPU-bound and synchronous, so each runs in a worker thread via
    :func:`asyncio.to_thread`; results keep the input rule order.
    """
    rule_list = list(rules)

    async def _run_one(rule: Rule) -> RuleResult:
        return await asyncio.to_thread(rule.check, records)

    results = await asyncio.gather(*(_run_one(r) for r in rule_list))
    return QualityReport(results=list(results), total_records=len(records))
