"""Declarative schema validation with type coercion.

A :class:`Schema` is an ordered set of :class:`Field` definitions. Validating
a record does two things:

* **Coerces** raw values (usually strings from CSV) into the field's declared
  Python type — ``int``, ``Decimal``, ``date``, ``datetime``, ``bool`` …
* **Checks** constraints (required, nullable, min/max, length, regex, allowed
  values) and reports *every* violation, not just the first, so a bad extract
  can be fixed in one pass.

Validation never mutates the input record; it returns a new, coerced dict.
Schemas can also be built from / exported to plain JSON-compatible dicts,
which is what the CLI uses for ``--schema schema.json``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field as dc_field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

__all__ = ["Field", "Schema", "SchemaError", "FieldError"]


class SchemaError(ValueError):
    """Raised when a record fails schema validation.

    Carries the full list of :class:`FieldError` objects in ``errors``.
    """

    def __init__(self, errors: list[FieldError]) -> None:
        self.errors = errors
        summary = "; ".join(str(e) for e in errors)
        super().__init__(f"{len(errors)} schema violation(s): {summary}")


@dataclass(frozen=True)
class FieldError:
    """One validation failure on one field."""

    field: str
    code: str  # machine-readable: missing | null | type | range | length | pattern | choice
    message: str

    def __str__(self) -> str:
        return f"{self.field}: {self.message}"


_TRUE_VALUES = {"true", "1", "yes", "y", "t"}
_FALSE_VALUES = {"false", "0", "no", "n", "f"}

SUPPORTED_TYPES = ("str", "int", "float", "decimal", "bool", "date", "datetime")


def _coerce(value: Any, type_name: str) -> Any:
    """Coerce ``value`` to ``type_name`` or raise ValueError."""
    if type_name == "str":
        return value if isinstance(value, str) else str(value)
    if type_name == "int":
        if isinstance(value, bool):  # bool is an int subclass — reject explicitly
            raise ValueError("boolean is not an int")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return int(str(value).strip())
    if type_name == "float":
        if isinstance(value, bool):
            raise ValueError("boolean is not a float")
        return float(value)
    if type_name == "decimal":
        if isinstance(value, Decimal):
            return value
        if isinstance(value, float):
            # via str() to avoid binary float artefacts (0.1 -> Decimal('0.1'))
            return Decimal(str(value))
        return Decimal(str(value).strip())
    if type_name == "bool":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE_VALUES:
            return True
        if text in _FALSE_VALUES:
            return False
        raise ValueError(f"not a boolean: {value!r}")
    if type_name == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value).strip())
    if type_name == "datetime":
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    raise ValueError(f"unknown type: {type_name!r}")


@dataclass(frozen=True)
class Field:
    """Schema definition for a single column.

    Attributes:
        name: Column name as it appears in the source record.
        type: One of :data:`SUPPORTED_TYPES`.
        required: The key must be present in the record.
        nullable: An explicit ``None`` / empty-string value is acceptable.
        default: Value used when an optional field is absent (or null).
        min_value / max_value: Inclusive numeric bounds (int/float/Decimal fields).
        min_length / max_length: Length bounds for string fields.
        pattern: Regex the (string) value must fully match.
        choices: Closed set of acceptable values (checked after coercion).
    """

    name: str
    type: str = "str"
    required: bool = True
    nullable: bool = False
    default: Any = None
    min_value: float | Decimal | None = None
    max_value: float | Decimal | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    choices: tuple[Any, ...] | None = None

    def __post_init__(self) -> None:
        if self.type not in SUPPORTED_TYPES:
            raise ValueError(
                f"field {self.name!r}: unsupported type {self.type!r}; "
                f"expected one of {SUPPORTED_TYPES}"
            )


@dataclass
class Schema:
    """An ordered collection of fields with record-level validation."""

    fields: list[Field]
    strict: bool = False  # strict=True rejects keys not present in the schema
    _by_name: dict[str, Field] = dc_field(init=False, repr=False)

    def __post_init__(self) -> None:
        names = [f.name for f in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("duplicate field names in schema")
        self._by_name = {f.name: f for f in self.fields}

    # -- validation ------------------------------------------------------

    def validate(self, record: dict[str, Any]) -> dict[str, Any]:
        """Return a coerced copy of ``record`` or raise :class:`SchemaError`."""
        errors: list[FieldError] = []
        out: dict[str, Any] = {}

        if self.strict:
            for key in record:
                if key not in self._by_name:
                    errors.append(
                        FieldError(key, "unexpected", f"unexpected field {key!r}")
                    )

        for f in self.fields:
            if f.name not in record:
                if f.required:
                    errors.append(FieldError(f.name, "missing", "required field is missing"))
                elif f.default is not None:
                    out[f.name] = f.default
                continue

            raw = record[f.name]
            if raw is None or (isinstance(raw, str) and raw == "" and f.type != "str"):
                if f.nullable:
                    out[f.name] = None
                elif f.default is not None:
                    out[f.name] = f.default
                else:
                    errors.append(FieldError(f.name, "null", "value is null but field is not nullable"))
                continue

            try:
                value = _coerce(raw, f.type)
            except (ValueError, InvalidOperation) as exc:
                errors.append(
                    FieldError(f.name, "type", f"cannot coerce {raw!r} to {f.type}: {exc}")
                )
                continue

            self._check_constraints(f, value, errors)
            out[f.name] = value

        if errors:
            raise SchemaError(errors)
        return out

    def _check_constraints(self, f: Field, value: Any, errors: list[FieldError]) -> None:
        if f.min_value is not None and isinstance(value, (int, float, Decimal)):
            if value < f.min_value:
                errors.append(FieldError(f.name, "range", f"{value} < min {f.min_value}"))
        if f.max_value is not None and isinstance(value, (int, float, Decimal)):
            if value > f.max_value:
                errors.append(FieldError(f.name, "range", f"{value} > max {f.max_value}"))
        if isinstance(value, str):
            if f.min_length is not None and len(value) < f.min_length:
                errors.append(
                    FieldError(f.name, "length", f"length {len(value)} < min {f.min_length}")
                )
            if f.max_length is not None and len(value) > f.max_length:
                errors.append(
                    FieldError(f.name, "length", f"length {len(value)} > max {f.max_length}")
                )
            if f.pattern is not None and not re.fullmatch(f.pattern, value):
                errors.append(
                    FieldError(f.name, "pattern", f"{value!r} does not match {f.pattern!r}")
                )
        if f.choices is not None and value not in f.choices:
            errors.append(
                FieldError(f.name, "choice", f"{value!r} not in allowed values {f.choices!r}")
            )

    def partition(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], SchemaError]]]:
        """Split records into (valid coerced, [(original, error), ...]).

        This is the dead-letter pattern: good rows keep flowing, bad rows are
        quarantined with their errors instead of killing the whole batch.
        """
        valid: list[dict[str, Any]] = []
        invalid: list[tuple[dict[str, Any], SchemaError]] = []
        for record in records:
            try:
                valid.append(self.validate(record))
            except SchemaError as exc:
                invalid.append((record, exc))
        return valid, invalid

    # -- (de)serialisation -------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "strict": self.strict,
            "fields": [
                {
                    "name": f.name,
                    "type": f.type,
                    "required": f.required,
                    "nullable": f.nullable,
                    "default": f.default,
                    "min_value": str(f.min_value) if isinstance(f.min_value, Decimal) else f.min_value,
                    "max_value": str(f.max_value) if isinstance(f.max_value, Decimal) else f.max_value,
                    "min_length": f.min_length,
                    "max_length": f.max_length,
                    "pattern": f.pattern,
                    "choices": list(f.choices) if f.choices is not None else None,
                }
                for f in self.fields
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Schema:
        fields: list[Field] = []
        for raw in data["fields"]:
            kwargs = dict(raw)
            if kwargs.get("choices") is not None:
                kwargs["choices"] = tuple(kwargs["choices"])
            fields.append(Field(**kwargs))
        return cls(fields=fields, strict=bool(data.get("strict", False)))
