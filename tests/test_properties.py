"""Property-based tests (hypothesis).

Unit tests check examples I thought of; properties check invariants that
must hold for *every* input — which is where pipeline bugs actually hide
(weird keys, empty batches, reordered columns, re-run safety).
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from pipeline_toolkit import transforms
from pipeline_toolkit.loaders import content_hash
from pipeline_toolkit.schema import Field, Schema

records_with_ids = st.lists(
    st.fixed_dictionaries(
        {"id": st.integers(min_value=0, max_value=20), "v": st.text(max_size=5)}
    ),
    max_size=40,
)


@given(records_with_ids)
def test_dedup_output_keys_are_unique(records):
    out = transforms.dedup(records, key="id")
    ids = [r["id"] for r in out]
    assert len(ids) == len(set(ids))


@given(records_with_ids)
def test_dedup_is_idempotent(records):
    once = transforms.dedup(records, key="id")
    twice = transforms.dedup(once, key="id")
    assert once == twice


@given(records_with_ids)
def test_dedup_never_invents_or_loses_keys(records):
    out = transforms.dedup(records, key="id")
    assert {r["id"] for r in out} == {r["id"] for r in records}


@given(st.dictionaries(st.text(min_size=1, max_size=4), st.integers(), max_size=8))
def test_content_hash_is_key_order_independent(record):
    shuffled = dict(reversed(list(record.items())))
    assert content_hash([record]) == content_hash([shuffled])


@given(
    st.lists(
        st.fixed_dictionaries(
            {
                "n": st.integers(min_value=0, max_value=10_000),
                "label": st.text(max_size=10),
            }
        ),
        max_size=25,
    )
)
@settings(max_examples=50)
def test_schema_validation_is_a_fixed_point(records):
    """Validating an already-validated record changes nothing — pipelines
    that validate twice (extract + load) must converge, not drift."""
    schema = Schema([Field("n", "int", min_value=0), Field("label", "str")])
    for record in records:
        once = schema.validate(dict(record))
        twice = schema.validate(dict(once))
        assert once == twice
