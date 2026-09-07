from __future__ import annotations

from datetime import datetime, timezone

import pytest

from load_test import (
    IntegrityError,
    ScenarioResult,
    percentile,
    reconcile,
    render_results_markdown,
)


def test_percentiles_and_deadline_percentage_match_known_samples() -> None:
    samples = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0]

    assert percentile(samples, 50.0) == 450.0
    assert percentile(samples, 95.0) == pytest.approx(765.0)
    assert percentile(samples, 99.0) == pytest.approx(793.0)
    assert percentile(samples, 99.9) == pytest.approx(799.3)
    assert sum(value <= 500.0 for value in samples) / len(samples) * 100 == 62.5


def test_reconciliation_accepts_one_assessment_and_expected_alert_per_input() -> None:
    reconciliation = reconcile(
        sent_event_ids={"normal", "suspicious"},
        assessments={"normal": "NOT_SUSPICIOUS", "suspicious": "SUSPICIOUS"},
        assessment_duplicates=set(),
        alert_event_ids={"suspicious"},
        alert_duplicates=set(),
    )

    assert reconciliation.inputs == 2
    assert reconciliation.assessments == 2
    assert reconciliation.alerts == 1
    assert reconciliation.missing_assessments == 0
    assert reconciliation.unexpected_alerts == 0


@pytest.mark.parametrize(
    ("assessments", "assessment_duplicates", "alerts", "alert_duplicates", "message"),
    [
        ({"one": "NOT_SUSPICIOUS"}, set(), set(), set(), "missing assessments"),
        (
            {"one": "NOT_SUSPICIOUS", "two": "SUSPICIOUS"},
            {"two"},
            {"two"},
            set(),
            "duplicate assessments",
        ),
        (
            {"one": "NOT_SUSPICIOUS", "two": "SUSPICIOUS"},
            set(),
            {"one", "two"},
            set(),
            "unexpected alerts",
        ),
        (
            {"one": "NOT_SUSPICIOUS", "two": "SUSPICIOUS"},
            set(),
            {"two"},
            {"two"},
            "duplicate alerts",
        ),
    ],
)
def test_reconciliation_rejects_loss_or_duplicate_effects(
    assessments: dict[str, str],
    assessment_duplicates: set[str],
    alerts: set[str],
    alert_duplicates: set[str],
    message: str,
) -> None:
    with pytest.raises(IntegrityError, match=message):
        reconcile(
            sent_event_ids={"one", "two"},
            assessments=assessments,
            assessment_duplicates=assessment_duplicates,
            alert_event_ids=alerts,
            alert_duplicates=alert_duplicates,
        )


def test_report_states_each_capacity_target_without_extrapolation() -> None:
    result = ScenarioResult.synthetic(
        name="sustained-8000",
        target_tps=8_000,
        duration_seconds=1.0,
        sent=7_500,
        alerts=75,
        durable_latencies_ms=[100.0, 200.0],
        producer_latencies_ms=[120.0, 220.0],
        max_consumer_lag=12,
    )

    report = render_results_markdown(
        executed_at=datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc),
        seed=8,
        environment={"host": "test-host"},
        configuration={
            "partitions": 12,
            "stream_threads": 1,
            "containers": "kafka=image@sha256:abc; detection-engine=fraud-engine:local",
            "ruleset": "version=1, hash=sha256:def, rule=AMOUNT_THRESHOLD(10000 BRL)",
        },
        results=[result],
    )

    assert "NAO ATINGIDA" in report
    assert "7.500,00 TPS" in report
    assert "Este ensaio local nao certifica capacidade produtiva" in report
    assert "p99,9" in report
    assert "containers" in report
    assert "AMOUNT_THRESHOLD(10000 BRL)" in report
