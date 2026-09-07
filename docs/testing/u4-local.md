# U4 local: Kafka Streams

Este roteiro executa o motor de deteccao como aplicacao Spring local e usa os utilitarios Kafka do Compose. Ele nao precisa de PostgreSQL para avaliar uma transacao; o ruleset e recebido pelo topico compactado.

## 1. Preparar Java, artefato e Kafka

No diretorio raiz do repositorio:

```bash
export JAVA_HOME=/Users/adeagro/Library/Java/JavaVirtualMachines/ms-21.0.8/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

./mvnw -pl services/detection-engine -am package -DskipTests
docker compose --env-file .env.example up -d --wait
docker compose --env-file .env.example run --rm kafka-init
```

## 2. Iniciar o motor

Em um terminal, escolha um identificador novo para que o estado local da demonstracao seja isolado e inicie o processo:

```bash
export RUN_ID="$(date +%s)"
export KAFKA_BOOTSTRAP_SERVERS=localhost:9094
export DETECTION_KAFKA_APPLICATION_ID="u4-local-$RUN_ID"
export DETECTION_PORT=8081

java -jar services/detection-engine/target/detection-engine-0.1.0-SNAPSHOT.jar
```

Em outro terminal, a saude comeca `DOWN` se nao houver snapshot valido no topico e passa a incluir `streamsReadiness.loadedVersion` depois da carga:

```bash
curl --silent http://localhost:8081/actuator/health
```

## 3. Publicar o ruleset dinamico

No mesmo shell que definiu `RUN_ID`, gere um snapshot valido contendo uma regra sem estado e uma `COUNT_WINDOW`.

O motor valida o JSON Schema, a versao, o hash SHA-256 do snapshot e a DSL antes de trocar o ruleset ativo. Por isso o fixture abaixo gera o `contentHash` a partir da representacao canonica do snapshot sem o proprio campo `contentHash`.

```bash
export RULESET_VERSION="$RUN_ID"
export NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

RULESET_RECORD="$(
python3 <<'PY'
import hashlib
import json
import os

snapshot = {
    "schemaVersion": 1,
    "snapshotId": f"u4-local-{os.environ['RUN_ID']}",
    "version": int(os.environ["RULESET_VERSION"]),
    "approvedChangeRuleVersionId": "u4-local-rule",
    "rules": [
        {
            "ruleId": "amount-10000",
            "ruleVersion": 1,
            "evaluationOrder": 0,
            "severity": "HIGH",
            "definition": {
                "type": "AMOUNT_THRESHOLD",
                "amountMinor": 10000,
                "currency": "BRL"
            }
        },
        {
            "ruleId": "two-in-ten-minutes",
            "ruleVersion": 1,
            "evaluationOrder": 1,
            "severity": "MEDIUM",
            "definition": {
                "type": "COUNT_WINDOW",
                "minimumCount": 2,
                "windowSeconds": 600
            }
        }
    ],
    "createdAt": os.environ["NOW"]
}

canonical_unsigned = json.dumps(
    snapshot,
    sort_keys=True,
    separators=(",", ":")
)

snapshot["contentHash"] = (
    "sha256:"
    + hashlib.sha256(
        canonical_unsigned.encode("utf-8")
    ).hexdigest()
)

print(
    "ACTIVE|"
    + json.dumps(
        snapshot,
        separators=(",", ":")
    )
)
PY
)"

printf '%s\n' "$RULESET_RECORD" \
  | docker compose --env-file .env.example exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic fraud.ruleset.active.v1 \
      --property parse.key=true \
      --property key.separator='|'

until curl --silent http://localhost:8081/actuator/health \
  | grep -q "\"loadedVersion\":$RULESET_VERSION"; do
  sleep 1
done

curl --silent http://localhost:8081/actuator/health
```

## 4. Abrir consumidores read-committed

Antes de produzir eventos, abra cada consumidor em seu proprio terminal. Eles aguardam somente as mensagens novas e encerram depois da quantidade esperada.

```bash
docker compose --env-file .env.example exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud.assessment.created.v1 \
  --consumer-property isolation.level=read_committed --max-messages 3 \
  --property print.key=true --property key.separator='|'
```

```bash
docker compose --env-file .env.example exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud.alert.internal.v1 \
  --consumer-property isolation.level=read_committed --max-messages 2 \
  --property print.key=true --property key.separator='|'
```

```bash
docker compose --env-file .env.example exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud.notification.requested.v1 \
  --consumer-property isolation.level=read_committed --max-messages 2 \
  --property print.key=true --property key.separator='|'
```

## 5. Produzir tres transacoes

No shell que possui `RUN_ID`, publique primeiro o evento normal. Espere o consumidor de avaliacoes mostrar `NOT_SUSPICIOUS` antes de enviar os dois seguintes: a ordem de chegada entre chaves de transacao distintas nao e garantida antes do reparticionamento, e uma regra temporal nunca usa um fato futuro.

```bash
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

printf '%s\n' \
  "tx-normal-$RUN_ID|{\"schemaVersion\":1,\"eventId\":\"event-normal-$RUN_ID\",\"transactionId\":\"tx-normal-$RUN_ID\",\"customerId\":\"customer-count-$RUN_ID\",\"amountMinor\":100,\"currency\":\"BRL\",\"occurredAt\":\"$NOW\",\"transactionType\":\"PURCHASE\",\"channel\":\"APP\",\"merchantCountry\":\"BR\",\"deviceIdHash\":\"device\",\"traceId\":\"trace-normal-$RUN_ID\"}" \
  | docker compose --env-file .env.example exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic fraud.transaction.received.v1 \
      --property parse.key=true \
      --property key.separator='|'
```

Depois que a primeira avaliacao aparecer, publique a transacao que ativa `COUNT_WINDOW` e a que ativa o limite de valor:

```bash
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

printf '%s\n' \
  "tx-count-$RUN_ID|{\"schemaVersion\":1,\"eventId\":\"event-count-$RUN_ID\",\"transactionId\":\"tx-count-$RUN_ID\",\"customerId\":\"customer-count-$RUN_ID\",\"amountMinor\":100,\"currency\":\"BRL\",\"occurredAt\":\"$NOW\",\"transactionType\":\"PURCHASE\",\"channel\":\"APP\",\"merchantCountry\":\"BR\",\"deviceIdHash\":\"device\",\"traceId\":\"trace-count-$RUN_ID\"}" \
  "tx-amount-$RUN_ID|{\"schemaVersion\":1,\"eventId\":\"event-amount-$RUN_ID\",\"transactionId\":\"tx-amount-$RUN_ID\",\"customerId\":\"customer-amount-$RUN_ID\",\"amountMinor\":12000,\"currency\":\"BRL\",\"occurredAt\":\"$NOW\",\"transactionType\":\"PURCHASE\",\"channel\":\"APP\",\"merchantCountry\":\"BR\",\"deviceIdHash\":\"device\",\"traceId\":\"trace-amount-$RUN_ID\"}" \
  | docker compose --env-file .env.example exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic fraud.transaction.received.v1 \
      --property parse.key=true \
      --property key.separator='|'
```

Os consumidores mostram tres avaliacoes, duas saidas de alerta e duas solicitacoes de notificacao. As chaves observaveis sao, respectivamente, `transactionId`, `alertId` e `notificationRequestId`.

O fluxo conserva `transactionId` ate a verificacao de identidade. Em seguida, reparticiona uma unica vez por `customerId` para os stores de deduplicacao e historico. Esses stores persistentes possuem changelogs Kafka; o ruleset e um global store compactado. Com `exactly_once_v2`, atualizacao de estado e publicacoes de saida do processamento confirmado formam uma unica transacao Kafka; por isso os consumidores do roteiro usam `read_committed`.

Para encerrar a infraestrutura sem apagar os volumes:

```bash
docker compose --env-file .env.example down
```
