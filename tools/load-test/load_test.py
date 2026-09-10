#!/usr/bin/env python3
"""Reproducible Kafka load test for the fraud detection engine."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Protocol, Sequence, TypeVar

TRANSACTION_TOPIC = "fraud.transaction.received.v1"
RULESET_TOPIC = "fraud.ruleset.active.v1"
ASSESSMENT_TOPIC = "fraud.assessment.created.v1"
ALERT_TOPIC = "fraud.alert.internal.v1"
OUTPUT_CONSUME_BATCH_SIZE = 1_000
MessageT = TypeVar("MessageT")


class BatchConsumer(Protocol[MessageT]):
    def consume(self, *, num_messages: int, timeout: float) -> list[MessageT]: ...


def consume_output_batches(
    consumer: BatchConsumer[MessageT],
    stop: threading.Event,
    handle: Callable[[list[MessageT]], None],
) -> None:
    while not stop.is_set():
        messages = consumer.consume(num_messages=OUTPUT_CONSUME_BATCH_SIZE, timeout=0.05)
        if messages:
            handle(messages)


class IntegrityError(RuntimeError):
    """Input and output identities did not reconcile."""


def percentile(samples: Sequence[float], requested_percentile: float) -> float:
    if not samples:
        raise ValueError("percentile requires at least one sample")
    if not 0 <= requested_percentile <= 100:
        raise ValueError("percentile must be between 0 and 100")
    ordered = sorted(samples)
    position = (len(ordered) - 1) * requested_percentile / 100
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _percentile_from_ordered(samples: Sequence[float], requested_percentile: float) -> float:
    position = (len(samples) - 1) * requested_percentile / 100
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return float(samples[lower])
    return samples[lower] + (samples[upper] - samples[lower]) * (position - lower)


@dataclass(frozen=True)
class Reconciliation:
    inputs: int
    assessments: int
    alerts: int
    missing_assessments: int
    unexpected_assessments: int
    duplicate_assessments: int
    missing_alerts: int
    unexpected_alerts: int
    duplicate_alerts: int


def reconcile(
    *,
    sent_event_ids: set[str],
    assessments: dict[str, str],
    assessment_duplicates: set[str],
    alert_event_ids: set[str],
    alert_duplicates: set[str],
) -> Reconciliation:
    assessed = set(assessments)
    expected_alerts = {key for key, status in assessments.items() if status == "SUSPICIOUS"}
    result = Reconciliation(
        len(sent_event_ids),
        len(assessed),
        len(alert_event_ids),
        len(sent_event_ids - assessed),
        len(assessed - sent_event_ids),
        len(assessment_duplicates),
        len(expected_alerts - alert_event_ids),
        len(alert_event_ids - expected_alerts),
        len(alert_duplicates),
    )
    failures = []
    for label, count in (
        ("missing assessments", result.missing_assessments),
        ("unexpected assessments", result.unexpected_assessments),
        ("duplicate assessments", result.duplicate_assessments),
        ("missing alerts", result.missing_alerts),
        ("unexpected alerts", result.unexpected_alerts),
        ("duplicate alerts", result.duplicate_alerts),
    ):
        if count:
            failures.append(f"{label}={count}")
    if failures:
        raise IntegrityError(", ".join(failures))
    return result


@dataclass(frozen=True)
class LatencySummary:
    p50: float
    p95: float
    p99: float
    p999: float
    maximum: float
    within_500ms_percent: float

    @classmethod
    def from_samples(cls, samples: Sequence[float]) -> "LatencySummary":
        if not samples:
            raise ValueError("latency summary requires at least one sample")
        ordered = sorted(samples)
        return cls(
            _percentile_from_ordered(ordered, 50),
            _percentile_from_ordered(ordered, 95),
            _percentile_from_ordered(ordered, 99),
            _percentile_from_ordered(ordered, 99.9),
            ordered[-1],
            sum(sample <= 500 for sample in samples) / len(samples) * 100,
        )


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    target_tps: int
    duration_seconds: float
    sent: int
    alerts: int
    producer_tps: float
    observed_tps: float
    assessment_end_to_end_latency: LatencySummary
    alert_end_to_end_latency: LatencySummary
    assessment_post_evaluation_latency: LatencySummary
    alert_post_evaluation_latency: LatencySummary
    max_consumer_lag: int
    reconciliation: Reconciliation

    @property
    def throughput_met(self) -> bool:
        return self.observed_tps >= self.target_tps

    @property
    def latency_met(self) -> bool:
        return (
            self.assessment_end_to_end_latency.within_500ms_percent >= 99.9
            and self.alert_end_to_end_latency.within_500ms_percent >= 99.9
        )


@dataclass(frozen=True)
class Scenario:
    name: str
    target_tps: int
    duration_seconds: float


def _number(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _render_latency_row(label: str, summary: LatencySummary) -> str:
    values = (summary.p50, summary.p95, summary.p99, summary.p999, summary.maximum)
    return f"| {label} | " + " | ".join(f"{_number(value)} ms" for value in values) + " |"


def render_results_markdown(
    *,
    executed_at: datetime,
    seed: int,
    environment: dict[str, object],
    configuration: dict[str, object],
    results: Sequence[ScenarioResult],
) -> str:
    lines = [
        "# Resultados do benchmark de capacidade",
        "",
        f"Executado em `{executed_at.astimezone(timezone.utc).isoformat()}` com semente fixa `{seed}`.",
        "",
        "> Este ensaio local nao certifica capacidade produtiva. Os numeros valem somente para o ambiente e a configuracao registrados abaixo.",
        "",
        "## Ambiente",
        "",
        *(f"- **{key}:** `{value}`" for key, value in environment.items()),
        "",
        "## Configuracao",
        "",
        *(f"- **{key}:** `{value}`" for key, value in configuration.items()),
        "",
        "## Resultados",
        "",
        "| Cenario | Meta | Envio | Vazao observada | avaliacoes <= 500 ms | alertas <= 500 ms | Vazao | Latencia E2E |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for result in results:
        lines.append(
            f"| {result.name} | {_number(result.target_tps)} TPS | {_number(result.producer_tps)} TPS | "
            f"{_number(result.observed_tps)} TPS | "
            f"{_number(result.assessment_end_to_end_latency.within_500ms_percent)}% | "
            f"{_number(result.alert_end_to_end_latency.within_500ms_percent)}% | "
            f"{'ATINGIDA' if result.throughput_met else 'NAO ATINGIDA'} | "
            f"{'ATINGIDA' if result.latency_met else 'NAO ATINGIDA'} |"
        )
    lines.extend(["", "## Distribuicao de latencia", ""])
    for result in results:
        lines.extend(
            [
                f"### {result.name}",
                "",
                "| Medida | p50 | p95 | p99 | p99,9 | maximo |",
                "|---|---:|---:|---:|---:|---:|",
                _render_latency_row(
                    "avaliacao E2E: observacao `read_committed` - envio aceito pelo produtor",
                    result.assessment_end_to_end_latency,
                ),
                _render_latency_row(
                    "alerta E2E: observacao `read_committed` - envio aceito pelo produtor",
                    result.alert_end_to_end_latency,
                ),
                _render_latency_row(
                    "diagnostico da avaliacao: observacao `read_committed` - `evaluatedAt`",
                    result.assessment_post_evaluation_latency,
                ),
                _render_latency_row(
                    "diagnostico do alerta: observacao `read_committed` - `createdAt`",
                    result.alert_post_evaluation_latency,
                ),
                "",
                f"Integridade: {result.reconciliation.inputs} entradas unicas, "
                f"{result.reconciliation.assessments} avaliacoes, {result.alerts} alertas, "
                "zero perdas e zero duplicacoes. "
                f"Maior atraso de consumo observado: {result.max_consumer_lag} registros.",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretacao",
            "",
            "A vazao observada considera o intervalo entre o primeiro envio e a observacao da ultima avaliacao/alerta esperado. A meta de latencia usa, separadamente para avaliacoes e alertas, o intervalo monotono entre o envio aceito pelo produtor e a observacao da saida por um consumidor `read_committed`. Ela inclui fila do produtor, entrada no Kafka, reparticionamento, espera da tarefa, avaliacao, commit transacional, saida no Kafka e atraso do observador; por isso e um limite superior conservador do tempo ponta a ponta deste ensaio. As medidas iniciadas em `evaluatedAt`/`createdAt` sao apenas diagnosticas e nao decidem a meta de 500 ms.",
            "",
        ]
    )
    return "\n".join(lines)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_ruleset() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "schemaVersion": 1,
        "snapshotId": str(uuid.uuid4()),
        "version": 1,
        "approvedChangeRuleVersionId": str(uuid.uuid4()),
        "rules": [
            {
                "ruleId": "u8-high-amount",
                "ruleVersion": 1,
                "evaluationOrder": 0,
                "severity": "HIGH",
                "definition": {"type": "AMOUNT_THRESHOLD", "amountMinor": 10000, "currency": "BRL"},
            }
        ],
        "createdAt": _now().isoformat().replace("+00:00", "Z"),
    }
    body = json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {**unsigned, "contentHash": f"sha256:{hashlib.sha256(body.encode()).hexdigest()}"}


def _kafka_types():
    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except ImportError as error:
        raise SystemExit("Install tools/load-test/requirements.txt first") from error
    return Consumer, KafkaError, Producer


class ScenarioRunner:
    def __init__(self, bootstrap: str, seed: int, alert_every: int, timeout: float):
        Consumer, self.kafka_error, Producer = _kafka_types()
        run_id = uuid.uuid4().hex
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": f"fraud-u8-producer-{run_id}",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 5,
                "batch.num.messages": 10000,
                "compression.type": "lz4",
            }
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"fraud-u8-reader-{run_id}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        self.seed, self.alert_every, self.timeout = seed, alert_every, timeout

    def publish_ruleset(self) -> dict[str, object]:
        ruleset = build_ruleset()
        value = json.dumps(ruleset, separators=(",", ":")).encode()
        self.producer.produce(RULESET_TOPIC, key=b"ACTIVE", value=value)
        if self.producer.flush(10):
            raise RuntimeError("ruleset publication did not flush")
        return ruleset

    def _start_at_end(self) -> None:
        assigned = threading.Event()

        def on_assign(consumer, partitions):
            for partition in partitions:
                _, partition.offset = consumer.get_watermark_offsets(partition, timeout=10)
            consumer.assign(partitions)
            assigned.set()

        self.consumer.subscribe([ASSESSMENT_TOPIC, ALERT_TOPIC], on_assign=on_assign)
        deadline = time.monotonic() + 15
        while not assigned.is_set() and time.monotonic() < deadline:
            self.consumer.poll(0.1)
        if not assigned.is_set():
            raise RuntimeError("consumer did not receive an assignment")

    def run(self, scenario: Scenario) -> ScenarioResult:
        self._start_at_end()
        prefix = f"u8-{scenario.name}-{uuid.uuid4().hex[:10]}"
        rng = random.Random(self.seed)
        total = max(1, round(scenario.target_tps * scenario.duration_seconds))
        sent: set[str] = set()
        sent_at: dict[str, float] = {}
        assessments: dict[str, str] = {}
        assessment_duplicates: set[str] = set()
        alerts: set[str] = set()
        alert_duplicates: set[str] = set()
        assessment_end_to_end: list[float] = []
        alert_end_to_end: list[float] = []
        assessment_post_evaluation: list[float] = []
        alert_post_evaluation: list[float] = []
        max_lag = 0
        observed_messages = 0
        suspicious_assessments = 0
        started = time.perf_counter()
        first_sent = started
        last_output = started
        state_lock = threading.Lock()
        observer_stop = threading.Event()
        observer_errors: list[Exception] = []

        def handle_messages(messages: list[object]) -> None:
            nonlocal max_lag, last_output, observed_messages, suspicious_assessments
            observed_perf, observed_at = time.perf_counter(), _now()
            for message in messages:
                if message.error():
                    if message.error().code() != self.kafka_error._PARTITION_EOF:
                        raise RuntimeError(str(message.error()))
                    continue
                document = json.loads(message.value())
                event_id = document.get("eventId")
                sample_lag = False
                with state_lock:
                    if event_id not in sent:
                        continue
                    observed_messages += 1
                    end_to_end_ms = (observed_perf - sent_at[event_id]) * 1000
                    if message.topic() == ASSESSMENT_TOPIC:
                        if event_id in assessments:
                            assessment_duplicates.add(event_id)
                        else:
                            assessments[event_id] = document["status"]
                            suspicious_assessments += document["status"] == "SUSPICIOUS"
                            assessment_end_to_end.append(end_to_end_ms)
                            assessment_post_evaluation.append(
                                max(
                                    0.0,
                                    (observed_at - _instant(document["evaluatedAt"]))
                                    .total_seconds()
                                    * 1000,
                                )
                            )
                            last_output = observed_perf
                    elif message.topic() == ALERT_TOPIC:
                        if event_id in alerts:
                            alert_duplicates.add(event_id)
                        else:
                            alerts.add(event_id)
                            alert_end_to_end.append(end_to_end_ms)
                            alert_post_evaluation.append(
                                max(
                                    0.0,
                                    (observed_at - _instant(document["createdAt"]))
                                    .total_seconds()
                                    * 1000,
                                )
                            )
                            last_output = observed_perf
                    sample_lag = observed_messages % 1_000 == 0
                if sample_lag:
                    lag = 0
                    partitions = self.consumer.assignment()
                    positions = self.consumer.position(partitions)
                    for partition, position in zip(partitions, positions, strict=True):
                        _, high = self.consumer.get_watermark_offsets(partition, cached=True)
                        lag += max(0, high - position.offset) if position.offset >= 0 else 0
                    with state_lock:
                        max_lag = max(max_lag, lag)

        def observe_outputs() -> None:
            try:
                consume_output_batches(self.consumer, observer_stop, handle_messages)
            except Exception as error:
                with state_lock:
                    observer_errors.append(error)

        def raise_observer_error() -> None:
            with state_lock:
                if observer_errors:
                    raise observer_errors[0]

        observer = threading.Thread(target=observe_outputs, name="load-test-output-observer")
        observer.start()
        try:
            for index in range(total):
                target_at = started + index / scenario.target_tps
                while (remaining := target_at - time.perf_counter()) > 0:
                    self.producer.poll(0)
                    time.sleep(min(remaining, 0.001))
                event_id, transaction_id = f"{prefix}-e-{index}", f"{prefix}-t-{index}"
                payload = {
                    "schemaVersion": 1,
                    "eventId": event_id,
                    "transactionId": transaction_id,
                    "customerId": f"u8-customer-{rng.randrange(10_000):05d}",
                    "amountMinor": (
                        15000 if index % self.alert_every == 0 else rng.randrange(100, 9900)
                    ),
                    "currency": "BRL",
                    "occurredAt": _now().isoformat().replace("+00:00", "Z"),
                    "transactionType": "PURCHASE",
                    "channel": "APP",
                    "merchantCountry": "BR",
                    "deviceIdHash": f"sha256:u8-{rng.randrange(1_000_000):06d}",
                    "traceId": f"{prefix}-trace-{index}",
                }
                while True:
                    accepted_at = time.perf_counter()
                    with state_lock:
                        sent.add(event_id)
                        sent_at[event_id] = accepted_at
                    try:
                        self.producer.produce(
                            TRANSACTION_TOPIC,
                            key=transaction_id.encode(),
                            value=json.dumps(payload, separators=(",", ":")).encode(),
                        )
                        break
                    except BufferError:
                        with state_lock:
                            sent.remove(event_id)
                            del sent_at[event_id]
                        self.producer.poll(0.01)
                    except Exception:
                        with state_lock:
                            sent.remove(event_id)
                            del sent_at[event_id]
                        raise
                if index == 0:
                    first_sent = accepted_at
                if index % 100 == 0:
                    raise_observer_error()
            production_finished = time.perf_counter()
            if self.producer.flush(self.timeout):
                raise RuntimeError("producer did not flush all records")
            deadline, complete_since = time.monotonic() + self.timeout, None
            while time.monotonic() < deadline:
                raise_observer_error()
                with state_lock:
                    complete = (
                        len(assessments) == total and len(alerts) == suspicious_assessments
                    )
                if complete and complete_since is None:
                    complete_since = time.monotonic()
                if complete_since and time.monotonic() - complete_since >= 1:
                    break
                time.sleep(0.01)
        finally:
            observer_stop.set()
            observer.join(timeout=5)
        if observer.is_alive():
            raise RuntimeError("output observer did not stop")
        raise_observer_error()
        integrity = reconcile(
            sent_event_ids=sent,
            assessments=assessments,
            assessment_duplicates=assessment_duplicates,
            alert_event_ids=alerts,
            alert_duplicates=alert_duplicates,
        )
        return ScenarioResult(
            scenario.name,
            scenario.target_tps,
            scenario.duration_seconds,
            total,
            len(alerts),
            total / max(production_finished - first_sent, 0.000001),
            total / max(last_output - first_sent, 0.000001),
            LatencySummary.from_samples(assessment_end_to_end),
            LatencySummary.from_samples(alert_end_to_end),
            LatencySummary.from_samples(assessment_post_evaluation),
            LatencySummary.from_samples(alert_post_evaluation),
            max_lag,
            integrity,
        )

    def close(self) -> None:
        self.consumer.close()


def collect_environment() -> dict[str, object]:
    environment: dict[str, object] = {
        "host": socket.gethostname(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
    }
    if sys.platform == "darwin":
        for key, command in {
            "cpu": ["sysctl", "-n", "machdep.cpu.brand_string"],
            "memory_bytes": ["sysctl", "-n", "hw.memsize"],
        }.items():
            try:
                environment[key] = subprocess.check_output(command, text=True).strip()
            except (OSError, subprocess.CalledProcessError):
                environment[key] = "indisponivel"
    try:
        environment["docker_server"] = subprocess.check_output(
            ["docker", "version", "--format", "{{.Server.Version}}"], text=True
        ).strip()
        environment["docker_memory_bytes"] = subprocess.check_output(
            ["docker", "info", "--format", "{{.MemTotal}}"], text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        environment["docker"] = "indisponivel"
    return environment


def collect_containers(compose_project: str) -> str:
    try:
        output = subprocess.check_output(
            [
                "docker",
                "ps",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
                "--format",
                "{{.Names}}={{.Image}} ({{.ID}})",
            ],
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "indisponivel"
    return "; ".join(sorted(output.splitlines())) or "nenhum container encontrado"


def describe_ruleset(ruleset: dict[str, object] | None) -> str:
    if ruleset is None:
        return "preexistente; identidade nao publicada por esta execucao"
    rule = ruleset["rules"][0]
    definition = rule["definition"]
    return (
        f"snapshot={ruleset['snapshotId']}, version={ruleset['version']}, "
        f"hash={ruleset['contentHash']}, rule={definition['type']}"
        f"({definition['amountMinor']} {definition['currency']})"
    )


def wait_for_ruleset(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                health = json.load(response)
            details = health.get("components", {}).get("streamsReadiness", {})
            if details.get("status") == "UP" and details.get("details", {}).get("loadedVersion", 0) > 0:
                return
        except (OSError, urllib.error.URLError, json.JSONDecodeError):
            pass
        time.sleep(0.25)
    raise RuntimeError(f"ruleset readiness not reached within {timeout}s: {url}")


def parse_scenario(value: str) -> Scenario:
    try:
        name, tps, duration = value.split(":")
        result = Scenario(name, int(tps), float(duration))
    except ValueError as error:
        raise argparse.ArgumentTypeError("use NAME:TPS:SECONDS") from error
    if result.target_tps <= 0 or result.duration_seconds <= 0:
        raise argparse.ArgumentTypeError("TPS and SECONDS must be positive")
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--seed", type=int, default=20260907)
    parser.add_argument("--alert-every", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--publish-ruleset", action="store_true")
    parser.add_argument("--readiness-url", default="http://127.0.0.1:8081/actuator/health")
    parser.add_argument("--compose-project", default="fraud-engine-u8")
    parser.add_argument("--scenario", action="append", type=parse_scenario, dest="scenarios")
    parser.add_argument("--report", type=Path, default=Path("docs/performance/results.md"))
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.alert_every <= 0:
        parser.error("--alert-every must be positive")
    scenarios = args.scenarios or [
        Scenario("warmup", 1_000, 5),
        Scenario("sustained-8000", 8_000, 30),
        Scenario("peak-25000", 25_000, 5),
    ]
    runner = ScenarioRunner(args.bootstrap_servers, args.seed, args.alert_every, args.timeout)
    results = []
    published_ruleset = None
    try:
        if args.publish_ruleset:
            published_ruleset = runner.publish_ruleset()
            wait_for_ruleset(args.readiness_url, min(args.timeout, 60))
        for scenario in scenarios:
            print(f"Running {scenario.name}: {scenario.target_tps} TPS for {scenario.duration_seconds}s", flush=True)
            result = runner.run(scenario)
            results.append(result)
            print(
                f"  sent={result.sent} assessments={result.reconciliation.assessments} alerts={result.alerts} "
                f"observed={result.observed_tps:.2f} TPS "
                f"assessment-e2e-p99.9={result.assessment_end_to_end_latency.p999:.2f} ms "
                f"alert-e2e-p99.9={result.alert_end_to_end_latency.p999:.2f} ms",
                flush=True,
            )
    finally:
        runner.close()
    configuration = {
        "bootstrap_servers": args.bootstrap_servers,
        "partitions": 12,
        "stream_threads": 1,
        "processing_guarantee": "exactly_once_v2",
        "consumer_isolation": "read_committed",
        "output_consume_batch_size": OUTPUT_CONSUME_BATCH_SIZE,
        "scenario_timeout_seconds": args.timeout,
        "containers": collect_containers(args.compose_project),
        "ruleset": describe_ruleset(published_ruleset),
        "alert_every": args.alert_every,
        "scenarios": ", ".join(
            f"{item.name}:{item.target_tps}:{item.duration_seconds}s" for item in scenarios
        ),
    }
    report = render_results_markdown(
        executed_at=_now(),
        seed=args.seed,
        environment=collect_environment(),
        configuration=configuration,
        results=results,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    print(f"Report written to {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
