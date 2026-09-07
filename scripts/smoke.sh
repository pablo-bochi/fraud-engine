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

mailpit_total_is_one() {
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

  [[ "$total" == "1" ]]
}

echo "==> Cleaning previous smoke stack"
cleanup
trap cleanup EXIT

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

RULE_ID="$(
  printf '%s' "$CREATE_RESPONSE" |
    json_field "ruleId"
)"

RULE_VERSION_ID="$(
  printf '%s' "$CREATE_RESPONSE" |
    json_field "ruleVersionId"
)"

echo "==> Approving smoke rule"

curl -fsS \
  -X POST \
  -H "Authorization: Bearer ${APPROVER_TOKEN}" \
  "http://localhost:${CONTROL_PORT}/api/v1/rules/${RULE_ID}/versions/${RULE_VERSION_ID}/approval" \
  >/dev/null

detection_ready() {
  curl -fsS \
    "http://localhost:${DETECTION_PORT}/actuator/health" |
    grep -Fq '"status":"UP"'
}

echo "==> Waiting for active ruleset in detection-engine"
wait_until "detection-engine ruleset readiness" detection_ready

NOW="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
SUFFIX="$(date +%s)"

NORMAL_TX="smoke-normal-${SUFFIX}"
NORMAL_EVENT="smoke-normal-event-${SUFFIX}"

SUSPICIOUS_TX="smoke-suspicious-${SUFFIX}"
SUSPICIOUS_EVENT="smoke-suspicious-event-${SUFFIX}"

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

ASSESSMENTS="$(consume_topic fraud.assessment.created.v1)"

printf '%s' "$ASSESSMENTS" |
  grep -F "$NORMAL_TX" |
  grep -Fq '"status":"NOT_SUSPICIOUS"' \
  || fail "Normal transaction was not NOT_SUSPICIOUS"

printf '%s' "$ASSESSMENTS" |
  grep -F "$SUSPICIOUS_TX" |
  grep -Fq '"status":"SUSPICIOUS"' \
  || fail "Suspicious transaction was not SUSPICIOUS"

echo "==> Waiting for alert"

wait_until \
  "internal alert" \
  topic_contains \
  fraud.alert.internal.v1 \
  "$SUSPICIOUS_TX"

echo "==> Waiting for notification request"

wait_until \
  "notification request" \
  topic_contains \
  fraud.notification.requested.v1 \
  "$SUSPICIOUS_TX"

NOTIFICATION_LINES="$(
  consume_topic fraud.notification.requested.v1 |
    grep -F "$SUSPICIOUS_TX"
)"

NOTIFICATION_LINE="$(
  printf '%s\n' "$NOTIFICATION_LINES" |
    tail -n 1
)"

NOTIFICATION_REQUEST_ID="${NOTIFICATION_LINE%%|*}"
NOTIFICATION_PAYLOAD="${NOTIFICATION_LINE#*|}"

[[ -n "$NOTIFICATION_REQUEST_ID" ]] \
  || fail "notificationRequestId not found"

echo "==> Waiting for notification result"

wait_until \
  "notification result" \
  topic_contains \
  fraud.notification.result.v1 \
  "$NOTIFICATION_REQUEST_ID"

RESULTS="$(
  consume_topic fraud.notification.result.v1
)"

printf '%s' "$RESULTS" |
  grep -F "$NOTIFICATION_REQUEST_ID" |
  grep -Fq '"status":"SENT"' \
  || fail "Notification result was not SENT"

echo "==> Waiting for Mailpit message"
wait_until "one Mailpit message" mailpit_total_is_one

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

[[ "$MAILPIT_TOTAL" == "1" ]] \
  || fail "Expected one Mailpit message after replay, got $MAILPIT_TOTAL"

echo
echo "SMOKE PASSED"
echo "  normal assessment:      NOT_SUSPICIOUS"
echo "  suspicious assessment:  SUSPICIOUS"
echo "  internal alert:          observed"
echo "  notification request:    $NOTIFICATION_REQUEST_ID"
echo "  notification result:     SENT + replayed"
echo "  delivery rows:           1"
echo "  delivery attempts:       1"
echo "  Mailpit messages:        1"
