from datetime import date
from decimal import Decimal

import pytest

from pipeline_toolkit.schema import Field, Schema, SchemaError


def payments_schema() -> Schema:
    return Schema(
        [
            Field("payment_id", "str", pattern=r"pay_[0-9]+"),
            Field("amount", "decimal", min_value=0),
            Field("currency", "str", choices=("USD", "EUR")),
            Field("paid_on", "date"),
            Field("note", "str", required=False, nullable=True),
        ]
    )


def test_validate_coerces_types():
    out = payments_schema().validate(
        {"payment_id": "pay_1", "amount": "12.50", "currency": "USD", "paid_on": "2026-10-28"}
    )
    assert out["amount"] == Decimal("12.50")
    assert out["paid_on"] == date(2026, 10, 28)


def test_validate_collects_all_errors():
    with pytest.raises(SchemaError) as excinfo:
        payments_schema().validate(
            {"payment_id": "oops", "amount": "-3", "currency": "JPY", "paid_on": "not-a-date"}
        )
    codes = {e.code for e in excinfo.value.errors}
    assert codes == {"pattern", "range", "choice", "type"}


def test_missing_required_field():
    with pytest.raises(SchemaError) as excinfo:
        payments_schema().validate({"amount": "1", "currency": "USD", "paid_on": "2026-01-01"})
    assert excinfo.value.errors[0].code == "missing"


def test_null_handling_nullable_vs_not():
    schema = payments_schema()
    out = schema.validate(
        {"payment_id": "pay_1", "amount": "1", "currency": "USD",
         "paid_on": "2026-01-01", "note": ""}
    )
    assert out["note"] == ""  # str field: empty string is a value, not null
    with pytest.raises(SchemaError):
        schema.validate(
            {"payment_id": "pay_1", "amount": "", "currency": "USD", "paid_on": "2026-01-01"}
        )


def test_default_applied_for_absent_optional():
    schema = Schema([Field("a", "int"), Field("b", "str", required=False, default="n/a")])
    assert schema.validate({"a": "5"}) == {"a": 5, "b": "n/a"}


def test_bool_coercion_and_int_rejects_bool():
    schema = Schema([Field("flag", "bool"), Field("n", "int")])
    assert schema.validate({"flag": "yes", "n": "3"}) == {"flag": True, "n": 3}
    with pytest.raises(SchemaError):
        schema.validate({"flag": True, "n": True})


def test_decimal_from_float_has_no_binary_artefacts():
    schema = Schema([Field("x", "decimal")])
    assert schema.validate({"x": 0.1})["x"] == Decimal("0.1")


def test_strict_mode_rejects_unexpected_keys():
    schema = Schema([Field("a", "int")], strict=True)
    with pytest.raises(SchemaError) as excinfo:
        schema.validate({"a": 1, "surprise": 2})
    assert excinfo.value.errors[0].code == "unexpected"


def test_length_constraints():
    schema = Schema([Field("code", "str", min_length=2, max_length=3)])
    with pytest.raises(SchemaError):
        schema.validate({"code": "abcd"})
    assert schema.validate({"code": "ab"}) == {"code": "ab"}


def test_partition_separates_good_from_bad():
    schema = Schema([Field("n", "int", min_value=0)])
    valid, invalid = schema.partition([{"n": "1"}, {"n": "-1"}, {"n": "2"}])
    assert valid == [{"n": 1}, {"n": 2}]
    assert len(invalid) == 1
    assert invalid[0][0] == {"n": "-1"}


def test_dict_roundtrip():
    schema = payments_schema()
    assert Schema.from_dict(schema.to_dict()).to_dict() == schema.to_dict()


def test_unknown_field_type_rejected():
    with pytest.raises(ValueError, match="unsupported type"):
        Field("x", "uuid")  # type: ignore[arg-type]
