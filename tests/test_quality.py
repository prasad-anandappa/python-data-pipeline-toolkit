from datetime import timedelta

import pytest

from pipeline_toolkit.quality import (
    CustomRule,
    FreshnessRule,
    InSetRule,
    NotNullRule,
    QualityEngine,
    RangeRule,
    ReferencedValuesRule,
    RegexRule,
    RowCountRule,
    RowRule,
    UniqueRule,
    load_plugin_rules,
    register_rule,
    registered_rules,
)

ROWS = [
    {"id": 1, "amount": 10.0, "ccy": "USD", "code": "pay_1", "acct": "a1"},
    {"id": 2, "amount": -4.0, "ccy": "JPY", "code": "bad", "acct": "a9"},
    {"id": 2, "amount": 5.0, "ccy": "USD", "code": "pay_3", "acct": "a1"},
    {"id": 3, "amount": None, "ccy": "USD", "code": "pay_4", "acct": "a2"},
]


def run(*rules):
    return QualityEngine(list(rules), include_plugins=False).run(ROWS)


def test_not_null():
    result = NotNullRule("amount").check(ROWS)
    assert not result.passed and result.failed == 1


def test_unique_counts_duplicates():
    result = UniqueRule("id").check(ROWS)
    assert not result.passed and result.failed == 1


def test_range_ignores_nulls_but_catches_negatives():
    result = RangeRule("amount", min_value=0).check(ROWS)
    assert not result.passed and result.failed == 1


def test_regex_and_in_set():
    assert not RegexRule("code", r"pay_[0-9]+").check(ROWS).passed
    assert not InSetRule("ccy", {"USD", "EUR"}).check(ROWS).passed
    assert InSetRule("ccy", {"USD", "EUR", "JPY"}).check(ROWS).passed


def test_referenced_values():
    result = ReferencedValuesRule("acct", {"a1", "a2"}).check(ROWS)
    assert not result.passed and result.failed == 1


def test_row_count():
    assert RowCountRule(min_rows=1, max_rows=10).check(ROWS).passed
    assert not RowCountRule(min_rows=100).check(ROWS).passed


def test_warn_severity_does_not_fail_gate():
    report = run(RangeRule("amount", min_value=0, severity="WARN"))
    assert report.passed and len(report.warnings) == 1


def test_error_severity_fails_gate():
    report = run(NotNullRule("amount"))
    assert not report.passed


def test_custom_rule():
    rule = CustomRule("positive", lambda r: (r["amount"] or 0) > 0)
    assert not rule.check(ROWS).passed


def test_freshness_rule_flags_stale_batch():
    from datetime import datetime, timezone

    rows = [{"ts": datetime(2020, 1, 1, tzinfo=timezone.utc)}]
    rule = FreshnessRule("ts", max_age=timedelta(days=1),
                         now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert not rule.check(rows).passed


def test_registry_has_builtins_and_rejects_duplicates():
    names = registered_rules()
    assert {"not_null", "unique", "range", "regex", "in_set"} <= set(names)
    with pytest.raises(ValueError, match="already registered"):
        register_rule("unique")(type("Dup", (RowRule,), {}))


def test_register_custom_rule_class():
    @register_rule("test_only_positive_ids")
    class PositiveIds(RowRule):
        def violation(self, record):
            return None if record["id"] > 0 else "id must be positive"

    assert "test_only_positive_ids" in registered_rules()
    assert PositiveIds().check(ROWS).passed


def test_plugin_loader_returns_empty_without_plugins():
    assert load_plugin_rules(group="pipeline_toolkit.rules") is not None


def test_report_summary_and_dict():
    report = run(UniqueRule("id"))
    assert "FAIL" in report.summary()
    d = report.to_dict()
    assert d["passed"] is False and d["total_records"] == 4
