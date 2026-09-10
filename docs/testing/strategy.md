# Estratégia de testes

## Objetivo

A suíte separa provas rápidas de domínio e contrato das integrações que exigem Docker. O smoke é a única prova que atravessa os três serviços, o broker, os dois schemas PostgreSQL e o SMTP local.

## Camadas

| Camada | Ferramenta e escopo | Exemplos cobertos |
|---|---|---|
| contrato | JUnit, Jackson e JSON Schema Draft-07 | exemplos válidos, campos obrigatórios, limites, compatibilidade record/schema |
| domínio | JUnit 5 e AssertJ | DSL, composição, janela, agregação e IDs determinísticos |
| REST/segurança | Spring MockMvc e chaves RSA efêmeras | assinatura, `iss`, `aud`, `exp`, `sub`, RBAC e rejeição de identidade no body |
| persistência | Testcontainers PostgreSQL | proposta/auditoria atômicas, snapshot/outbox, concorrência, rollback e entrega idempotente |
| Kafka Streams | `TopologyTestDriver` | ruleset dinâmico, stores, event-time, duplicata, conflito, roteamento de saídas e presença do reparticionamento por cliente |
| Kafka real | Testcontainers Kafka | processamento transacional, consumo `read_committed` e regra stateful reunindo o mesmo cliente vindo de partições distintas |
| SMTP | Testcontainers Mailpit | envio real e contenção do contato no notificador |
| ponta a ponta | Docker Compose + Bash | regras stateless/stateful, reparticionamento entre partições, avaliações, alertas, resultados, replay idempotente e dois e-mails legítimos |
| observabilidade | Compose, Prometheus e dashboard provisionado | scrape dos três serviços, métricas sem alta cardinalidade e inspeção local |
| desempenho opcional | Python, `confluent-kafka` e stack Compose reduzida | percentis, limite de 500 ms, vazão observada e reconciliação de entradas/avaliações/alertas |

## Comandos canônicos

```bash
# rápido, sem executar testes de integração
./mvnw spotless:check verify -DskipITs

# suíte completa, incluindo classes *IntegrationTest e *IT
./mvnw verify -Pintegration

# somente núcleo do motor
./mvnw -pl services/detection-engine -am test

# validação estática do ambiente local
docker compose --env-file .env.example config --quiet

# prova ponta a ponta em projeto Compose isolado
./scripts/smoke.sh

# testes puros e protocolo completo do benchmark de capacidade (fora do CI comum)
/tmp/fraud-engine-u8-venv/bin/python -m pytest tools/load-test/test_load_test.py -q
# veja docs/performance/benchmark-protocol.md antes de executar a carga
```

`-DskipITs` é necessário no caminho rápido porque os módulos vinculam classes `*IntegrationTest` ao Failsafe. O perfil `integration` ativa `integration-test` e `verify`.

## Critérios por comportamento

- Todo evento estruturalmente válido avaliado com ruleset carregado gera exatamente uma avaliação no caminho coberto.
- Eventos inválidos e conflitos geram referências sanitizadas e não alteram o histórico.
- Uma avaliação suspeita agrega todas as regras acionadas, escolhe a maior severidade e cria IDs determinísticos.
- Uma regra stateful sem histórico suficiente torna a avaliação inconclusiva quando nenhuma regra conclusiva acionou.
- O processamento com estado por cliente ocorre depois de um reparticionamento explícito; o teste com Kafka real envia o mesmo cliente por partições de origem distintas.
- Aprovação exige outro `sub`, não aceita identidade administrativa no payload, nunca cria conjunto vazio e persiste snapshot/outbox atomicamente.
- Publicação conserva ordem de versões e pode repetir o mesmo snapshot depois de falha entre Kafka e commit.
- Repetição de uma notificação já enviada mantém uma linha, uma tentativa e um SMTP.
- Consumidores que verificam atomicidade usam `read_committed`.

## Evidência de TDD

Os registros contemporâneos das unidades estão em [tdd-evidence.md](tdd-evidence.md). Eles descrevem vermelhos e verdes observados quando foram preservados; não transformam uma diferença de Git ou um teste verde tardio em um vermelho que não foi registrado.

O escopo planejado da U9 é documental, mas a verificação completa revelou uma regressão de
inicialização introduzida pela U6: o contexto in-memory do notificador não possui `DataSource`.
O componente de métricas passou a seguir a mesma propriedade que habilita a persistência,
preservando os gauges no contexto normal e omitindo-os no contexto in-memory. A estratégia da
unidade combina:

1. conferir cada afirmação contra código, configuração, migrations, scripts e histórico;
2. validar links relativos e blocos Mermaid;
3. executar os comandos documentados a partir da base limpa;
4. revisar README, Compose, tópicos e serviços de forma cruzada.

`NotificationDeliveryMetricsTest` cobre persistência habilitada e desabilitada; o teste de integração
`NotificationConsumerIntegrationTest` comprova o segundo caso no bootstrap real do serviço.

## Cenários adiados

- caos, perda de broker, restauração longa, múltiplas instâncias e failover PostgreSQL;
- bootstrap bloqueante até um end offset conhecido e convergência coordenada do ruleset;
- proteção de transações recebidas antes do primeiro ruleset válido; smoke e carga evitam esse caso ao carregar a regra primeiro, mas não provam pausa, buffer ou replay;
- DLQ automatizada, investigação/replay da quarentena e retenção protegida de payload bruto;
- lease de entrega, timeout `SENDING`, ambiguidade após aceite SMTP, circuit breaker e provedor externo;
- backtest/reprocessamento com `runId` e notificações bloqueadas;
- segurança AWS/IAM/TLS/KMS, rotação e testes de infraestrutura multi-AZ.

Esses itens são desenho ou evolução. A ausência deles não é encoberta por mocks nem por afirmações de capacidade.
