from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest

from load_test import (
    IntegrityError,
    LatencySummary,
    OUTPUT_CONSUME_BATCH_SIZE,
    ScenarioResult,
    consume_output_batches,
    percentile,
    reconcile,
    render_results_markdown,
)


def test_output_batches_are_consumed_on_a_dedicated_thread() -> None:
    stop = threading.Event()
    consumed_on: list[int] = []
    handled: list[list[str]] = []

    class ConsumerStub:
        def consume(self, *, num_messages: int, timeout: float) -> list[str]:
            assert num_messages == OUTPUT_CONSUME_BATCH_SIZE
            assert timeout == 0.05
            consumed_on.append(threading.get_ident())
            stop.set()
            return ["first", "second"]

    observer = threading.Thread(
        target=consume_output_batches,
        args=(ConsumerStub(), stop, handled.append),
    )
    observer.start()
    observer.join(timeout=1)

    assert not observer.is_alive()
    assert consumed_on != [threading.get_ident()]
    assert handled == [["first", "second"]]


def _synthetic_result(
    *,
    assessment_end_to_end: list[float],
    alert_end_to_end: list[float],
    assessment_post_evaluation: list[float],
    alert_post_evaluation: list[float],
    name: str = "synthetic",
    target_tps: int = 1,
    sent: int | None = None,
    alerts: int | None = None,
    max_consumer_lag: int = 0,
) -> ScenarioResult:
    sent = len(assessment_end_to_end) if sent is None else sent
    alerts = len(alert_end_to_end) if alerts is None else alerts
    return ScenarioResult(
        name=name,
        target_tps=target_tps,
        duration_seconds=1.0,
        sent=sent,
        alerts=alerts,
        producer_tps=float(sent),
        observed_tps=float(sent),
        assessment_end_to_end_latency=LatencySummary.from_samples(assessment_end_to_end),
        alert_end_to_end_latency=LatencySummary.from_samples(alert_end_to_end),
        assessment_post_evaluation_latency=LatencySummary.from_samples(
            assessment_post_evaluation
        ),
        alert_post_evaluation_latency=LatencySummary.from_samples(alert_post_evaluation),
        max_consumer_lag=max_consumer_lag,
        reconciliation=reconcile(
            sent_event_ids=set(),
            assessments={},
            assessment_duplicates=set(),
            alert_event_ids=set(),
            alert_duplicates=set(),
        ),
    )


def test_percentiles_and_deadline_percentage_match_known_samples() -> None:
    samples = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0]

    assert percentile(samples, 50.0) == 450.0
    assert percentile(samples, 95.0) == pytest.approx(765.0)
    assert percentile(samples, 99.0) == pytest.approx(793.0)
    assert percentile(samples, 99.9) == pytest.approx(799.3)
    assert sum(value <= 500.0 for value in samples) / len(samples) * 100 == 62.5


@pytest.mark.parametrize(
    ("assessment_end_to_end", "alert_end_to_end", "expected"),
    [
        ([100.0, 200.0], [100.0], True),
        ([100.0, 600.0], [100.0], False),
        ([100.0, 200.0], [600.0], False),
    ],
)
def test_latency_target_requires_both_end_to_end_outputs_to_meet_deadline(
    assessment_end_to_end: list[float],
    alert_end_to_end: list[float],
    expected: bool,
) -> None:
    result = _synthetic_result(
        assessment_end_to_end=assessment_end_to_end,
        alert_end_to_end=alert_end_to_end,
        assessment_post_evaluation=[700.0],
        alert_post_evaluation=[800.0],
    )

    assert result.latency_met is expected


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
    result = _synthetic_result(
        name="sustained-8000",
        target_tps=8_000,
        sent=7_500,
        alerts=75,
        max_consumer_lag=12,
        assessment_end_to_end=[120.0, 220.0],
        alert_end_to_end=[150.0],
        assessment_post_evaluation=[100.0, 200.0],
        alert_post_evaluation=[110.0],
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
    assert "envio aceito pelo produtor" in report
    assert "avaliacoes <= 500 ms" in report
    assert "alertas <= 500 ms" in report
    assert "receivedAt" not in report
    assert "containers" in report
    assert "AMOUNT_THRESHOLD(10000 BRL)" in report
    assert "U8" not in report
