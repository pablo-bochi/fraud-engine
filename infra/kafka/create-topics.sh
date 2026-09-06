#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="kafka:9092"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"
kafka_configs="/opt/kafka/bin/kafka-configs.sh"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local cleanup_policy="$3"
  local retention_ms="$4"
  local cleanup_value="$cleanup_policy"

  if [[ "$cleanup_policy" == *,* ]]; then
    cleanup_value="[$cleanup_policy]"
  fi

  "$kafka_topics" --bootstrap-server "$bootstrap_server" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1
  "$kafka_configs" --bootstrap-server "$bootstrap_server" --alter --entity-type topics \
    --entity-name "$topic" --add-config "cleanup.policy=$cleanup_value,retention.ms=$retention_ms"
}

create_topic fraud.transaction.received.v1 12 delete 604800000
create_topic fraud.ruleset.active.v1 1 compact -1
create_topic fraud.assessment.created.v1 12 delete 2592000000
create_topic fraud.alert.internal.v1 12 delete 2592000000
create_topic fraud.notification.requested.v1 12 delete 604800000
create_topic fraud.notification.result.v1 12 compact,delete 2592000000
create_topic fraud.transaction.invalid.v1 6 delete 604800000
create_topic fraud.transaction.quarantine.v1 6 delete 604800000
create_topic fraud.notification.dlq.v1 6 delete 604800000
