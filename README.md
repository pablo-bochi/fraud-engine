# Fraud Detection Engine

Fatia vertical de um motor assíncrono de detecção de fraude. Transações imutáveis entram pelo Kafka, regras governadas são publicadas sem redeploy, o motor produz avaliações explicáveis e solicitações suspeitas terminam em uma entrega fictícia no Mailpit.

O repositório separa três aplicações Java 21:

- `fraud-control-service`: autoria, aprovação por quatro olhos, auditoria e outbox de snapshots;
- `detection-engine`: Kafka Streams, regras sem estado e por janela, estado local e saídas atômicas;
- `notification-service`: resolução local do contato, registro idempotente e envio SMTP.

A visão completa, incluindo limites do MVP e desenho produtivo, está em [docs/architecture/overview.md](docs/architecture/overview.md).

## Pré-requisitos

- Java 21 (`java -version` deve indicar 21);
- Docker com Compose v2;
- Bash, `curl`, OpenSSL e Python 3;
- portas locais livres: `8080`, `8081`, `8083`, `8025`, `9090`, `9094` e `3000`.

Não copie `.env.example` para executar os comandos abaixo: o Compose o recebe explicitamente. As credenciais e os dados são apenas locais e fictícios.

Confirme que `JAVA_HOME` aponta para o JDK 21 antes do build. No macOS, por exemplo:

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Executar o Spotless com um JDK posterior pode falhar por incompatibilidade com a API interna do `javac`, mesmo que o código tenha `release` 21.

## Caminho mais curto: smoke completo

Partindo de um checkout limpo:

```bash
./mvnw spotless:check verify -DskipITs
docker compose --env-file .env.example config --quiet
./scripts/smoke.sh
```

O smoke limpa um projeto Compose isolado, compila as aplicações, gera chaves e JWTs locais, sobe a infraestrutura e os três serviços, cria e aprova uma regra stateless de valor e uma regra stateful de contagem. Além das duas transações do fluxo ponta a ponta, publica três transações de baixo valor do mesmo cliente em partições de origem diferentes. O resultado esperado contém:

```text
SMOKE PASSED
  stateless amount rule:       SUSPICIOUS
  stateful count rule:         NOT_SUSPICIOUS, NOT_SUSPICIOUS, SUSPICIOUS
  stateful source partitions:  2, 10, 9
  internal alerts:             2 observed
  notification result:         SENT + stateless replayed
  stateless delivery rows:     1
  stateless delivery attempts: 1
  Mailpit messages:            2
```

O replay da solicitação stateless produz novamente o mesmo resultado, mas mantém uma única linha e uma tentativa para essa entrega. O segundo e-mail pertence ao alerta stateful.

Para conservar a pilha ao final e inspecionar as evidências:

```bash
KEEP_SMOKE_STACK=1 ./scripts/smoke.sh
```

Abra:

- Mailpit: <http://localhost:8025>;
- Kafka UI: <http://localhost:8083>;
- Prometheus: <http://localhost:9090>;
- Grafana: <http://localhost:3000> (`admin` / `admin-local-only`);
- saúde do motor: <http://localhost:8081/actuator/health>.

No Kafka UI, os principais resultados estão em `fraud.assessment.created.v1`, `fraud.alert.internal.v1`, `fraud.notification.requested.v1` e `fraud.notification.result.v1`. Os comandos equivalentes pela CLI estão na seção seguinte.

Encerre e remova os volumes da pilha de smoke:

```bash
docker compose --project-name fraud-engine-smoke --env-file .env.example down -v --remove-orphans
```

## Inspeção pela CLI do Kafka

Com a pilha preservada pelo comando anterior, leia avaliações confirmadas:

```bash
docker compose --project-name fraud-engine-smoke --env-file .env.example exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic fraud.assessment.created.v1 \
  --from-beginning \
  --consumer-property isolation.level=read_committed \
  --property print.key=true \
  --property key.separator='|'
```

Troque apenas `--topic` para inspecionar alertas, solicitações e resultados. `Ctrl+C` encerra o consumidor.

## Execução manual

Para subir a plataforma sem executar o smoke:

```bash
./scripts/dev-bootstrap.sh
./mvnw package -DskipTests
docker compose --env-file .env.example up --build -d --wait
```

O bootstrap cria uma chave RSA e JWTs de uma hora em `.local/security/`, que é ignorado pelo Git. O Compose aguarda o inicializador idempotente de tópicos e aplica as migrations Flyway dos dois schemas PostgreSQL.

O Postman collection em [docs/rules_postman_collection/Fraud Engine.postman_collection.json](docs/rules_postman_collection/Fraud%20Engine.postman_collection.json) cobre a API de regras. Os tokens são:

- `.local/security/rule-author.jwt`: `RULE_WRITE` e `RULE_READ`;
- `.local/security/rule-approver.jwt`: `RULE_APPROVE` e `RULE_READ`;
- `.local/security/rule-auditor.jwt`: `AUDIT_READ`.

O fluxo manual do motor e seus consumidores `read_committed` está detalhado em [docs/testing/u4-local.md](docs/testing/u4-local.md).

Para encerrar sem apagar volumes:

```bash
docker compose --env-file .env.example down
```

## Testes

```bash
# schemas, contratos, compilação e testes sem Docker
./mvnw spotless:check verify -DskipITs

# integrações Testcontainers (Docker obrigatório)
./mvnw verify -Pintegration

# configuração local
docker compose --env-file .env.example config --quiet

# fluxo real Kafka -> regras -> avaliação -> alerta -> Mailpit
./scripts/smoke.sh
```

A finalidade de cada camada e o que ainda não é testado estão em [docs/testing/strategy.md](docs/testing/strategy.md). Os ciclos registrados durante a implementação estão em [docs/testing/tdd-evidence.md](docs/testing/tdd-evidence.md).

## Benchmark opcional de capacidade

O ensaio de carga fica fora do caminho comum de CI e exige uma stack limpa contendo apenas Kafka, o inicializador de tópicos e o motor. O protocolo reproduzível, incluindo criação do ambiente Python, aquecimento, carga sustentada, pico e limpeza, está em [docs/performance/benchmark-protocol.md](docs/performance/benchmark-protocol.md). Os resultados medidos nesta máquina estão em [docs/performance/results.md](docs/performance/results.md).

```bash
/tmp/fraud-engine-u8-venv/bin/python tools/load-test/load_test.py \
  --bootstrap-servers 127.0.0.1:9094 \
  --compose-project fraud-engine-u8 \
  --publish-ruleset \
  --timeout 600 \
  --report docs/performance/results.md
```

O relatório só é gravado após reconciliar cada entrada com exatamente uma avaliação e cada avaliação suspeita com exatamente um alerta. Os números locais não certificam capacidade produtiva.

## Contratos, tópicos e API

Os schemas JSON Draft-07 em `contracts/schemas` são a interface canônica; exemplos aceitos e rejeitados ficam em `contracts/examples`. O módulo `libs/contracts-java` contém os records de transporte e valida os schemas em runtime nas fronteiras relevantes.

| Interface | Papel |
|---|---|
| `POST /api/v1/rules` | cria regra e versão inicial pendente |
| `POST /api/v1/rules/{ruleId}/versions` | propõe alteração ou retirada |
| `POST /api/v1/rules/{ruleId}/versions/{versionId}/approval` | aprova por outra identidade e retorna `202` |
| `POST /api/v1/rules/{ruleId}/versions/{versionId}/rejection` | rejeita uma proposta |
| `GET /api/v1/rulesets/active` | mostra versões desejada/publicada e snapshot |
| `GET /api/v1/rulesets/active/outbox` | mostra o estado da publicação |
| `GET /api/v1/audit-events` | consulta auditoria append-only |

O inventário completo de tópicos, chaves e retenções está em [docs/architecture/overview.md](docs/architecture/overview.md#contratos-e-tópicos).

## Escopo honesto

O MVP executa o núcleo local, incluindo a pilha de observabilidade com Prometheus, Grafana e Kafka UI. O benchmark local reproduzível de capacidade atingiu a meta de 99,9% em até 500 ms, mas não atingiu as metas de vazão de 8.000 TPS e pico de 25.000 TPS no ambiente medido. O MVP não executa replay/backtest, operação automática da quarentena, segurança corporativa AWS, múltiplas instâncias coordenadas ou reconciliação do intervalo ambíguo do SMTP. A automação de CI foi removida do escopo desta entrega por decisão explícita; os comandos de verificação permanecem reproduzíveis localmente.

Veja [docs/limitations-and-evolution.md](docs/limitations-and-evolution.md) e [docs/ai-usage.md](docs/ai-usage.md).
