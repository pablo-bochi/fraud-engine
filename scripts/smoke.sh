#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE=".env.example"
PROJECT="fraud-engine-smoke"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

compose() {
  docker compose \
    --project-name "$PROJECT" \
    --env-file "$ENV_FILE" \
    "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

finish() {
  if [[ "${KEEP_SMOKE_STACK:-0}" == "1" ]]; then
    echo "==> Keeping smoke stack running for inspection"
  else
    cleanup
  fi
}

fail() {
  echo "SMOKE FAILED: $1" >&2

  echo >&2
  echo "==> notification-service logs" >&2
  compose logs --no-color --tail=120 notification-service >&2 || true

  echo >&2
  echo "==> notification delivery rows" >&2
  compose exec -T postgres \
    psql \
      -U "$POSTGRES_USER" \
      -d "$POSTGRES_DB" \
      -c "SELECT notification_request_id, status, attempt_count, last_error_code
          FROM notification.delivery;" \
    >&2 || true

  exit 1
}

json_field() {
  local field="$1"

  python3 -c "
import json, sys
value = json.load(sys.stdin)
print(value['$field'])
"
}

wait_until() {
  local description="$1"
  shift

  local deadline=$((SECONDS + 30))

  while (( SECONDS < deadline )); do
    if "$@"; then
      return 0
    fi

    sleep 1
  done

  fail "Timed out waiting for: $description"
}

consume_topic() {
  local topic="$1"

  compose exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$topic" \
    --from-beginning \
    --timeout-ms 1500 \
    --property print.key=true \
    --property "key.separator=|" \
    2>/dev/null || true
}

topic_contains() {
  local topic="$1"
  local value="$2"

  consume_topic "$topic" | grep -Fq "$value"
}

capture_notification_line() {
  local transaction_id="$1"
  local variable_name="$2"
  local line

  line="$(
    consume_topic fraud.notification.requested.v1 |
      grep -F "$transaction_id" |
      tail -n 1
  )"

  [[ -n "$line" ]] || return 1

  printf -v "$variable_name" '%s' "$line"
}

internal_repartition_topic_exists() {
  compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --list |
    grep -Fxq 'fraud-detection-engine-customer-repartition'
}

mailpit_total_is() {
  local expected="$1"
  local response

  response="$(
    curl -fsS \
      "http://localhost:${MAILPIT_UI_PORT}/api/v1/messages"
  )" || return 1

  local total

  total="$(
    printf '%s' "$response" |
      python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])'
  )"

  [[ "$total" == "$expected" ]]
}

echo "==> Cleaning previous smoke stack"
cleanup
trap finish EXIT

echo "==> Building applications"
./mvnw package -DskipTests

echo "==> Generating local JWTs"
./scripts/dev-bootstrap.sh >/dev/null

AUTHOR_TOKEN="$(cat .local/security/rule-author.jwt)"
APPROVER_TOKEN="$(cat .local/security/rule-approver.jwt)"

echo "==> Starting stack"
compose up --build -d

control_ready() {
  local code

  code="$(
    curl -sS \
      -o /dev/null \
      -w '%{http_code}' \
      -H "Authorization: Bearer ${AUTHOR_TOKEN}" \
      "http://localhost:${CONTROL_PORT}/api/v1/rules" \
      || true
  )"

  [[ "$code" == "200" ]]
}

echo "==> Waiting for fraud-control-service"
wait_until "fraud-control-service" control_ready

echo "==> Creating smoke rule"

CREATE_RESPONSE="$(
  curl -fsS \
    -X POST \
    -H "Authorization: Bearer ${AUTHOR_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{
      "ruleKey": "smoke-high-amount",
      "name": "Smoke high amount",
      "definition": {
        "type": "AMOUNT_THRESHOLD",
        "amountMinor": 10000,
        "currency": "BRL"
      }
    }' \
    "http://localhost:${CONTROL_PORT}/api/v1/rules"
)"

AMOUNT_RULE_ID="$(
  printf '%s' "$CREATE_RESPONSE" |
    json_field "ruleId"
)"

AMOUNT_RULE_VERSION_ID="$(
  printf '%s' "$CREATE_RESPONSE" |
    json_field "ruleVersionId"
)"

echo "==> Approving smoke rule"

curl -fsS \
  -X POST \
  -H "Authorization: Bearer ${APPROVER_TOKEN}" \
  "http://localhost:${CONTROL_PORT}/api/v1/rules/${AMOUNT_RULE_ID}/versions/${AMOUNT_RULE_VERSION_ID}/approval" \
  >/dev/null

echo "==> Creating stateful count rule"

COUNT_CREATE_RESPONSE="$(
  curl -fsS \
    -X POST \
    -H "Authorization: Bearer ${AUTHOR_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{
      "ruleKey": "smoke-three-in-window",
      "name": "Smoke three transactions in ten minutes",
      "definition": {
        "type": "COUNT_WINDOW",
        "windowSeconds": 600,
        "minimumCount": 3
      }
    }' \
    "http://localhost:${CONTROL_PORT}/api/v1/rules"
)"

COUNT_RULE_ID="$(
  printf '%s' "$COUNT_CREATE_RESPONSE" |
    json_field "ruleId"
)"

COUNT_RULE_VERSION_ID="$(
  printf '%s' "$COUNT_CREATE_RESPONSE" |
    json_field "ruleVersionId"
)"

echo "==> Approving stateful count rule"

COUNT_APPROVAL_RESPONSE="$(
  curl -fsS \
    -X POST \
    -H "Authorization: Bearer ${APPROVER_TOKEN}" \
    "http://localhost:${CONTROL_PORT}/api/v1/rules/${COUNT_RULE_ID}/versions/${COUNT_RULE_VERSION_ID}/approval"
)"

RULESET_VERSION="$(
  printf '%s' "$COUNT_APPROVAL_RESPONSE" |
    json_field "desiredVersion"
)"

detection_ready() {
  curl -fsS \
    "http://localhost:${DETECTION_PORT}/actuator/health" |
    grep -Fq "\"loadedVersion\":${RULESET_VERSION}"
}

echo "==> Waiting for active ruleset in detection-engine"
wait_until "detection-engine ruleset readiness" detection_ready
wait_until "customer repartition topic" internal_repartition_topic_exists

NOW="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
SUFFIX="$(date +%s)"

NORMAL_TX="smoke-normal-${SUFFIX}"
NORMAL_EVENT="smoke-normal-event-${SUFFIX}"

SUSPICIOUS_TX="smoke-suspicious-${SUFFIX}"
SUSPICIOUS_EVENT="smoke-suspicious-event-${SUFFIX}"

STATEFUL_TX_1="smoke-stateful-tx-1"
STATEFUL_TX_2="smoke-stateful-tx-2"
STATEFUL_TX_3="smoke-stateful-tx-3"

STATEFUL_EVENT_1="smoke-stateful-event-1"
STATEFUL_EVENT_2="smoke-stateful-event-2"
STATEFUL_EVENT_3="smoke-stateful-event-3"

STATEFUL_CUSTOMER="smoke-stateful-customer"

NORMAL_PAYLOAD="$(
  cat <<EOF
{"schemaVersion":1,"eventId":"${NORMAL_EVENT}","transactionId":"${NORMAL_TX}","customerId":"smoke-customer","amountMinor":5000,"currency":"BRL","occurredAt":"${NOW}","transactionType":"PURCHASE","channel":"APP","merchantCountry":"BR","deviceIdHash":"sha256:smoke-device","traceId":"smoke-trace-normal"}
EOF
)"

SUSPICIOUS_PAYLOAD="$(
  cat <<EOF
{"schemaVersion":1,"eventId":"${SUSPICIOUS_EVENT}","transactionId":"${SUSPICIOUS_TX}","customerId":"smoke-customer","amountMinor":15000,"currency":"BRL","occurredAt":"${NOW}","transactionType":"PURCHASE","channel":"APP","merchantCountry":"BR","deviceIdHash":"sha256:smoke-device","traceId":"smoke-trace-suspicious"}
EOF
)"

STATEFUL_PAYLOAD_1="$(
  cat <<EOF
{"schemaVersion":1,"eventId":"${STATEFUL_EVENT_1}","transactionId":"${STATEFUL_TX_1}","customerId":"${STATEFUL_CUSTOMER}","amountMinor":100,"currency":"BRL","occurredAt":"${NOW}","transactionType":"PURCHASE","channel":"APP","merchantCountry":"BR","deviceIdHash":"sha256:smoke-stateful-device","traceId":"smoke-trace-stateful-1"}
EOF
)"

STATEFUL_PAYLOAD_2="$(
  cat <<EOF
{"schemaVersion":1,"eventId":"${STATEFUL_EVENT_2}","transactionId":"${STATEFUL_TX_2}","customerId":"${STATEFUL_CUSTOMER}","amountMinor":100,"currency":"BRL","occurredAt":"${NOW}","transactionType":"PURCHASE","channel":"APP","merchantCountry":"BR","deviceIdHash":"sha256:smoke-stateful-device","traceId":"smoke-trace-stateful-2"}
EOF
)"

STATEFUL_PAYLOAD_3="$(
  cat <<EOF
{"schemaVersion":1,"eventId":"${STATEFUL_EVENT_3}","transactionId":"${STATEFUL_TX_3}","customerId":"${STATEFUL_CUSTOMER}","amountMinor":100,"currency":"BRL","occurredAt":"${NOW}","transactionType":"PURCHASE","channel":"APP","merchantCountry":"BR","deviceIdHash":"sha256:smoke-stateful-device","traceId":"smoke-trace-stateful-3"}
EOF
)"

produce_transaction() {
  local key="$1"
  local payload="$2"

  printf '%s|%s\n' "$key" "$payload" |
    compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 \
      --topic fraud.transaction.received.v1 \
      --property parse.key=true \
      --property "key.separator=|"
}

stateful_transactions_are_in_expected_partitions() {
  local transactions

  transactions="$(
    compose exec -T kafka \
      /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 \
      --topic fraud.transaction.received.v1 \
      --from-beginning \
      --timeout-ms 1500 \
      --property print.partition=true \
      --property print.key=true \
      --property "key.separator=|" \
      2>/dev/null
  )" || return 1

  printf '%s' "$transactions" | grep -F "Partition:2" | grep -Fq "${STATEFUL_TX_1}|" &&
    printf '%s' "$transactions" | grep -F "Partition:10" | grep -Fq "${STATEFUL_TX_2}|" &&
    printf '%s' "$transactions" | grep -F "Partition:9" | grep -Fq "${STATEFUL_TX_3}|"
}

assessments_have_expected_statuses() {
  local assessments

  assessments="$(consume_topic fraud.assessment.created.v1)"

  printf '%s' "$assessments" | grep -F "$NORMAL_TX" | grep -Fq '"status":"NOT_SUSPICIOUS"' &&
    printf '%s' "$assessments" | grep -F "$SUSPICIOUS_TX" | grep -Fq '"status":"SUSPICIOUS"' &&
    printf '%s' "$assessments" | grep -F "$STATEFUL_TX_1" | grep -Fq '"status":"NOT_SUSPICIOUS"' &&
    printf '%s' "$assessments" | grep -F "$STATEFUL_TX_2" | grep -Fq '"status":"NOT_SUSPICIOUS"' &&
    printf '%s' "$assessments" | grep -F "$STATEFUL_TX_3" | grep -Fq '"status":"SUSPICIOUS"'
}

notification_results_are_sent() {
  local results

  results="$(consume_topic fraud.notification.result.v1)"

  printf '%s' "$results" | grep -F "$NOTIFICATION_REQUEST_ID" | grep -Fq '"status":"SENT"' &&
    printf '%s' "$results" | grep -F "$STATEFUL_NOTIFICATION_REQUEST_ID" | grep -Fq '"status":"SENT"'
}

echo "==> Producing normal transaction"
produce_transaction "$NORMAL_TX" "$NORMAL_PAYLOAD"

echo "==> Producing suspicious transaction"
produce_transaction "$SUSPICIOUS_TX" "$SUSPICIOUS_PAYLOAD"

echo "==> Waiting for normal assessment"
wait_until \
  "normal assessment" \
  topic_contains \
  fraud.assessment.created.v1 \
  "$NORMAL_TX"

echo "==> Waiting for suspicious assessment"
wait_until \
  "suspicious assessment" \
  topic_contains \
  fraud.assessment.created.v1 \
  "$SUSPICIOUS_TX"

echo "==> Producing stateful transaction 1 (source partition 2)"
produce_transaction "$STATEFUL_TX_1" "$STATEFUL_PAYLOAD_1"
wait_until \
  "first stateful assessment" \
  topic_contains \
  fraud.assessment.created.v1 \
  "$STATEFUL_TX_1"

echo "==> Producing stateful transaction 2 (source partition 10)"
produce_transaction "$STATEFUL_TX_2" "$STATEFUL_PAYLOAD_2"
wait_until \
  "second stateful assessment" \
  topic_contains \
  fraud.assessment.created.v1 \
  "$STATEFUL_TX_2"

echo "==> Producing stateful transaction 3 (source partition 9)"
produce_transaction "$STATEFUL_TX_3" "$STATEFUL_PAYLOAD_3"
wait_until \
  "third stateful assessment" \
  topic_contains \
  fraud.assessment.created.v1 \
  "$STATEFUL_TX_3"

wait_until "stateful transactions in source partitions 2, 10 and 9" \
  stateful_transactions_are_in_expected_partitions

wait_until "expected stateless and stateful assessments" \
  assessments_have_expected_statuses

echo "==> Waiting for alert"

wait_until \
  "internal alert" \
  topic_contains \
  fraud.alert.internal.v1 \
  "$SUSPICIOUS_TX"

wait_until \
  "stateful internal alert" \
  topic_contains \
  fraud.alert.internal.v1 \
  "$STATEFUL_TX_3"

echo "==> Waiting for notification requests"

NOTIFICATION_LINE=""
wait_until \
  "stateless notification request" \
  capture_notification_line \
  "$SUSPICIOUS_TX" \
  NOTIFICATION_LINE

NOTIFICATION_REQUEST_ID="${NOTIFICATION_LINE%%|*}"
NOTIFICATION_PAYLOAD="${NOTIFICATION_LINE#*|}"

[[ -n "$NOTIFICATION_REQUEST_ID" ]] \
  || fail "notificationRequestId not found"

STATEFUL_NOTIFICATION_LINE=""
wait_until \
  "stateful notification request" \
  capture_notification_line \
  "$STATEFUL_TX_3" \
  STATEFUL_NOTIFICATION_LINE

STATEFUL_NOTIFICATION_REQUEST_ID="${STATEFUL_NOTIFICATION_LINE%%|*}"

[[ -n "$STATEFUL_NOTIFICATION_REQUEST_ID" ]] \
  || fail "Stateful notificationRequestId not found"

echo "==> Waiting for notification results"

wait_until "stateless and stateful notification results are SENT" \
  notification_results_are_sent

echo "==> Waiting for Mailpit messages"
wait_until "two Mailpit messages" mailpit_total_is 2

echo "==> Replaying the same notification request"

printf '%s|%s\n' \
  "$NOTIFICATION_REQUEST_ID" \
  "$NOTIFICATION_PAYLOAD" |
  compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic fraud.notification.requested.v1 \
    --property parse.key=true \
    --property "key.separator=|"

two_results_exist() {
  local count

  count="$(
    consume_topic fraud.notification.result.v1 |
      grep -F "$NOTIFICATION_REQUEST_ID" |
      wc -l |
      tr -d ' '
  )"

  [[ "$count" -ge 2 ]]
}

echo "==> Waiting for replayed result"
wait_until \
  "second NotificationResult" \
  two_results_exist

echo "==> Verifying idempotent registry"

DELIVERY_STATE="$(
  compose exec -T postgres \
    psql \
      -U "$POSTGRES_USER" \
      -d "$POSTGRES_DB" \
      -Atc "
        SELECT
          count(*) || ':' ||
          max(attempt_count) || ':' ||
          max(status)
        FROM notification.delivery
        WHERE notification_request_id = '${NOTIFICATION_REQUEST_ID}';
      "
)"

[[ "$DELIVERY_STATE" == "1:1:SENT" ]] \
  || fail "Unexpected delivery state: $DELIVERY_STATE"

MAILPIT_TOTAL="$(
  curl -fsS \
    "http://localhost:${MAILPIT_UI_PORT}/api/v1/messages" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])'
)"

[[ "$MAILPIT_TOTAL" == "2" ]] \
  || fail "Expected two Mailpit messages after replay, got $MAILPIT_TOTAL"

echo
echo "SMOKE PASSED"
echo "  stateless amount rule:       SUSPICIOUS"
echo "  stateful count rule:         NOT_SUSPICIOUS, NOT_SUSPICIOUS, SUSPICIOUS"
echo "  stateful source partitions:  2, 10, 9"
echo "  internal alerts:             2 observed"
echo "  stateless notification:      $NOTIFICATION_REQUEST_ID"
echo "  stateful notification:       $STATEFUL_NOTIFICATION_REQUEST_ID"
echo "  notification result:         SENT + stateless replayed"
echo "  stateless delivery rows:     1"
echo "  stateless delivery attempts: 1"
echo "  Mailpit messages:            2"
