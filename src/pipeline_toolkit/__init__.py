"""pipeline_toolkit — small, typed building blocks for reliable data pipelines.

The library covers the four stages every batch pipeline needs:

1. **Ingest**   — streaming CSV / JSON / JSONL readers (:mod:`pipeline_toolkit.io`)
2. **Validate** — declarative schemas with precise per-field errors
   (:mod:`pipeline_toolkit.schema`)
3. **Check**    — a data-quality rule engine (:mod:`pipeline_toolkit.quality`)
4. **Load**     — idempotent loaders that make pipeline re-runs safe
   (:mod:`pipeline_toolkit.loaders`)

Everything is standard-library only and fully typed.
"""

from pipeline_toolkit.pipeline import Pipeline, RunReport
from pipeline_toolkit.quality import QualityEngine, QualityReport
from pipeline_toolkit.schema import Field, Schema, SchemaError

__all__ = [
    "Field",
    "Pipeline",
    "QualityEngine",
    "QualityReport",
    "RunReport",
    "Schema",
    "SchemaError",
]

__version__ = "0.1.0"
