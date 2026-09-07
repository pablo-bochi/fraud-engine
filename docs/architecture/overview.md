# Arquitetura do Fraud Detection Engine

## Objetivo e fronteira

O sistema detecta padrões suspeitos depois que a transação foi emitida. Ele não autoriza, recusa nem interrompe uma transação: a disponibilidade do motor fica fora do caminho transacional. Cada evento aceito produz uma avaliação; somente avaliações suspeitas geram alerta interno e solicitação sanitizada de contato.

O MVP é um monorepo Java 21 com três aplicações Spring Boot, Kafka 4.2 em KRaft, PostgreSQL 17 e Mailpit. Prometheus, Grafana e Kafka UI compõem a observabilidade local. O ambiente usa dados e identidades fictícios.

## Visão principal

```mermaid
flowchart LR
  classDef implemented fill:#dcfce7,stroke:#15803d,color:#14532d
  classDef optional fill:#fef3c7,stroke:#b45309,color:#78350f
  classDef production fill:#e0e7ff,stroke:#4338ca,color:#312e81,stroke-dasharray: 5 5

  Operator[Autor / aprovador] -->|JWT + REST| Control[fraud-control-service]
  Control -->|snapshot + outbox atômicos| PG[(PostgreSQL)]
  Control -->|RuleSetSnapshot / ACTIVE| Kafka[(Kafka)]
  Producer[Sistema transacional] -->|TransactionEvent| Kafka
  Kafka --> Engine[detection-engine / Kafka Streams]
  Engine -->|TransactionAssessment| Kafka
  Engine -->|InternalAlert| Kafka
  Engine -->|CustomerNotificationRequested| Kafka
  Kafka --> Notifier[notification-service]
  Notifier -->|registro idempotente| PG
  Notifier -->|e-mail fictício| Mailpit[Mailpit]
  Notifier -->|NotificationResult| Kafka
  Services[3 serviços] --> Prometheus[Prometheus]
  Prometheus --> Grafana[Grafana]
  Kafka --> KafkaUI[Kafka UI]
  Kafka -. produção .-> MSK[Amazon MSK]
  Services -. produção .-> Compute[ECS ou EKS]
  PG -. produção .-> RDS[Aurora / RDS]
  Compute -. produção .-> AWSControls[IAM, TLS, KMS, Secrets Manager, OTel]

  class Control,PG,Kafka,Producer,Engine,Notifier,Mailpit implemented
  class Prometheus,Grafana,KafkaUI optional
  class MSK,Compute,RDS,AWSControls production
```

Verde representa o núcleo executável. Amarelo representa a pilha local de observabilidade, uma extensão opcional implementada neste repositório. Azul tracejado é arquitetura produtiva documentada, não evidência local.

## Fluxo de controle: regras governadas

1. Um JWT RS256 válido, com `iss`, `aud`, `exp` e `sub`, cria uma regra ou nova versão. A autoria vem exclusivamente de `sub`.
2. A proposta imutável fica em `PENDING_APPROVAL`; uma regra só pode ter uma pendência.
3. Outra identidade, com `RULE_APPROVE`, aprova. Autoaprovação é recusada e auditada.
4. Sob lock do `ruleset_head`, o serviço monta o conjunto completo, valida DSL, limites, schema, serialização canônica, hash, tamanho e a regra de conjunto não vazio.
5. A mesma transação PostgreSQL decide a versão, grava auditoria, snapshot, head desejado e outbox.
6. O relay publica sempre a menor versão pendente com chave `ACTIVE`. Falha aplica backoff e impede N+1 de ultrapassar N.
7. Cada motor valida schema, hash, versão e DSL antes de trocar sua referência imutável. Snapshot inválido ou antigo conserva o último conjunto válido.

`desiredVersion`, `publishedVersion` e `loadedVersion` tornam a convergência observável, mas o MVP não coordena um corte simultâneo entre várias instâncias.

## Fluxo de dados

1. `TransactionEvent` chega com chave `transactionId` e atributos mínimos.
2. Schema e correspondência entre chave e `transactionId` são validados. Falha gera apenas `InvalidEventReference` sanitizada.
3. Antes do reparticionamento, um store persistente identifica outro `eventId` para a mesma transação.
4. O stream é reparticionado uma vez por `customerId`. Stores RocksDB/changelog mantêm impressão digital do evento e histórico temporal.
5. O motor executa `AMOUNT_THRESHOLD`, `COUNT_WINDOW`, `ALL` e `ANY` no snapshot carregado.
6. Qualquer `MATCHED` produz `SUSPICIOUS`; sem match e com `NOT_EVALUATED`, `INCONCLUSIVE`; todos em `NO_MATCH`, `NOT_SUSPICIOUS`.
7. Kafka Streams `exactly_once_v2` abrange offset, estado e saídas. IDs determinísticos protegem as fronteiras externas.
8. Uma avaliação suspeita gera um único alerta consolidado e uma solicitação sem e-mail ou canal.
9. O notificador resolve o contato fictício internamente, disputa uma transição persistente para `SENDING`, envia e grava `SENT` ou `FAILED`. Replay de uma entrega `SENT` republica o resultado sem novo SMTP.

Eventos tardios ainda cobertos pelos 15 minutos de histórico são avaliados e marcados. Quando a evidência necessária já expirou, regras com estado retornam `NOT_EVALUATED`; o sistema não fabrica uma conclusão segura.

## Contratos e tópicos

| Tópico | Chave | Payload | Partições | Retenção local |
|---|---|---|---:|---|
| `fraud.transaction.received.v1` | `transactionId` | `TransactionEvent` | 12 | 7 dias |
| `fraud.ruleset.active.v1` | `ACTIVE` | `RuleSetSnapshot` | 1 | compactado |
| `fraud.assessment.created.v1` | `transactionId` | `TransactionAssessment` | 12 | 30 dias |
| `fraud.alert.internal.v1` | `alertId` | `InternalAlert` | 12 | 30 dias |
| `fraud.notification.requested.v1` | `notificationRequestId` | `CustomerNotificationRequested` | 12 | 7 dias |
| `fraud.notification.result.v1` | `notificationRequestId` | `NotificationResult` | 12 | compactado + 30 dias |
| `fraud.transaction.invalid.v1` | chave recebida | `InvalidEventReference` | 6 | 7 dias |
| `fraud.transaction.quarantine.v1` | `transactionId` | `QuarantinedEventReference` | 6 | 7 dias |
| `fraud.notification.dlq.v1` | reservada | sem produtor automático no MVP | 6 | 7 dias |

Schemas JSON Draft-07 e exemplos ficam em `contracts/`. Records Java são manuais para manter contrato e código legíveis. Consumidores de saídas transacionais devem configurar `isolation.level=read_committed`.

## Integração com sistemas internos

| Sistema | Fronteira e responsabilidade | Robustez e evolução | Situação nesta entrega |
|---|---|---|---|
| sistema transacional | publica `TransactionEvent`; mantém a autorização financeira fora do motor | schema versionado, chave por `transactionId`, Kafka e validação na entrada | produtor representado pelo smoke |
| antifraude interno | consome `InternalAlert` para investigação e correlação | contrato assíncrono, retenção própria e consumidor `read_committed` | tópico real; consumidor corporativo não implementado |
| cadastro de clientes | resolve contato somente depois da decisão suspeita | porta substituível, identidade de workload e timeout/circuit breaker em produção | fixture local, sem integração corporativa |
| canais do cliente | recebe uma solicitação idempotente por adaptador de canal | chave idempotente, retry controlado e reconciliação do aceite em produção | SMTP local via Mailpit |

Cada integração preserva um contrato versionado na borda para que sistemas internos evoluam sem
compartilhar o domínio do motor. ACLs de tópico, autenticação de workload e credenciais de banco ou
provedor pertencem ao desenho produtivo; o ambiente local usa apenas identidades e segredos
demonstrativos.

## Idempotência e consistência

- `assessmentId = UUIDv3("ASSESSMENT:" + eventId)`;
- `alertId = UUIDv3("ALERT:" + assessmentId)`;
- `notificationRequestId = UUIDv3("NOTIFICATION-REQUEST:" + alertId)`;
- a identidade transacional é mantida por 24 horas de stream-time;
- um replay com mesma identidade e fingerprint não altera histórico nem republica efeitos;
- conflitos de `transactionId` ou fingerprint são isolados por referência sanitizada;
- outbox transacional liga aprovação no PostgreSQL à publicação repetível do snapshot;
- a PK `notification_request_id` e a transição condicional de estado impedem dois envios concluídos pelo caminho coberto.

Isso oferece efeitos efetivamente uma vez nas bordas demonstradas. Não é uma afirmação de exatamente uma vez global entre Kafka, SMTP e qualquer infraestrutura futura.

## Falhas e operação

| Falha | Comportamento executável | Endurecimento produtivo |
|---|---|---|
| serviço de controle indisponível | motor conserva o último snapshot carregado | SLO e alerta por idade/convergência |
| snapshot inválido/antigo | rejeitado sem substituir o ativo | quarentena operacional e investigação |
| Kafka indisponível | processamento pausa; não produz `INCONCLUSIVE` artificial | MSK multi-AZ, quotas e runbooks |
| payload inválido/conflitante | referência sanitizada em tópico separado; histórico intacto | retenção protegida e replay auditado |
| publicação da outbox falha | backoff limitado e ordenação preservada | alertas, lease e operação assistida |
| contato ou SMTP falha | entrega `FAILED` com código sanitizado e retry pela mesma identidade | circuit breaker, DLQ e reconciliação SMTP |
| processo reinicia | changelogs restauram stores do Kafka Streams | capacidade e tempo de restauração testados |

Prontidão do motor exige um ruleset válido. Os endpoints Actuator e as métricas não carregam `customerId`, contato ou payload como label. Logs devem usar somente IDs opacos de correlação.

### Resposta operacional de SRE

| Incidente prioritário | Sinais e alerta proposto | Primeira triagem e mitigação | Recuperação |
|---|---|---|---|
| aumento de falsos positivos | razão de `SUSPICIOUS`, regras acionadas e mudança recente de `loadedVersion`; alertar por desvio sustentado do baseline | correlacionar início com publicação de ruleset; suspender nova promoção e retirar a versão causadora pelo fluxo de quatro olhos | razão retorna à faixa esperada e nova avaliação amostral confirma a regra correta |
| queda de throughput ou aumento de latência | taxa e duração das avaliações, lag por partição, CPU, memória e disco; alertar antes de violar capacidade/SLO | localizar partição quente ou dependência lenta; limitar produtores quando necessário e escalar consumidores dentro do número de partições | lag converge, taxa de processamento supera a entrada e latência volta ao limite operacional |
| Kafka, PostgreSQL, contato ou SMTP indisponível | saúde da dependência, erros, idade da outbox, lag e entregas `FAILED`; alertar por duração e impacto | preservar o último ruleset, impedir avanço fora de ordem e repetir somente operações idempotentes; isolar o canal externo sem parar a detecção | dependência saudável, backlog drenado e contagens de entrada/saída reconciliadas sem duplicatas |

O dashboard local já expõe parte desses sinais, mas não implementa paging, baseline, SLO ou runbooks.
Esses itens, junto com responsáveis e canais de escalonamento, precisam ser definidos antes da
operação produtiva.

## Segurança e LGPD

No ambiente local, a API de controle é stateless, valida JWT assimétrico, separa permissões de escrita, aprovação, leitura e auditoria, e extrai identidades administrativas do token. O banco possui usuários e schemas separados para controle e notificação. O motor nunca recebe contato; a solicitação externa contém apenas `customerId`, categoria segura e template. A chave privada e tokens ficam em `.local/security/`, fora do Git.

Em produção, o desenho requer emissor corporativo/JWKS, TLS, IAM/ACL no MSK, identidade de workload, Secrets Manager e chaves gerenciadas pelo KMS para criptografia em repouso, além de trilha de auditoria protegida, retenção definida por finalidade, acesso mínimo e descarte verificável. Dados brutos de quarentena ou arquivo só podem ser acessados por processo autorizado. Esses controles AWS não foram executados localmente.

### Segurança ponta a ponta

| Enlace ou armazenamento | Dados mínimos | Identidade e autorização em produção | Proteção em trânsito e repouso |
|---|---|---|---|
| produtor -> Kafka | transação e identificadores | workload do produtor com permissão de escrita somente no tópico de entrada | TLS em trânsito; criptografia do broker com chaves gerenciadas |
| controle -> PostgreSQL/Kafka | regras, auditoria e snapshots sem contato do cliente | workload de controle, usuário de banco e ACLs restritos aos próprios recursos | TLS; volume e backup criptografados; segredos fora da imagem |
| Kafka -> motor | transações e rulesets | workload do motor com leitura de entrada/ruleset e escrita apenas nas saídas | TLS e tópicos criptografados no broker |
| Kafka -> notificador -> PostgreSQL | solicitação sanitizada e estado da entrega | workload do notificador e usuário do schema de notificação | TLS; banco e backup criptografados |
| notificador -> provedor de canal | contato resolvido e conteúdo mínimo | credencial dedicada e rotacionada no gerenciador de segredos | TLS; evitar persistência do contato fora do sistema responsável |
| serviços -> observabilidade | métricas agregadas e IDs opacos em logs | agentes de telemetria autenticados, acesso por função | TLS; retenção e armazenamento criptografados |

KMS gerencia as chaves da criptografia em repouso; TLS protege cada enlace em trânsito. A rotação de
segredos, certificados e chaves deve ser auditável. No ambiente local, Kafka usa `PLAINTEXT`, o SMTP
não representa um provedor real e os segredos são demonstrativos.

### Governança LGPD

| Categoria | Finalidade e minimização | Acesso, retenção e descarte propostos |
|---|---|---|
| evento financeiro | detectar fraude e produzir evidência explicável; não inclui contato | acesso restrito ao pipeline antifraude; prazo definido com Risco, Segurança e Jurídico; descarte verificável |
| identificadores de cliente/transação | correlação, idempotência e investigação | pseudonimização quando possível, acesso por função e retenção limitada à finalidade |
| contato do cliente | entregar a comunicação; permanece fora do motor | resolvido apenas no adaptador autorizado, não usado em labels/logs e descartado conforme política do cadastro/canal |
| regras e auditoria administrativa | governar decisões e atribuir ações | trilha protegida contra alteração, acesso de auditoria separado e retenção regulatória validada |
| quarentena e logs | diagnosticar falhas sem replicar payload sensível | referências sanitizadas por padrão; payload bruto somente em arquivo protegido, com acesso auditado e prazo curto |

A base legal, os prazos exatos, o atendimento aos direitos do titular e as responsabilidades por
controlador, operador e resposta a incidentes exigem validação das áreas Jurídica, Privacidade e
Segurança antes de produção. A documentação descreve o desenho técnico e não substitui essa avaliação.

## Escala e capacidade

O particionamento por cliente permite paralelismo por partição e mantém consulta de estado local. O tópico de entrada possui 12 partições no ambiente local; o teto real depende de distribuição de chaves, CPU, disco, tamanho dos stores, replicação e restauração. Chaves quentes podem exigir salting e agregação em dois estágios; esse desenho não está implementado.

As metas do case de 8.000 TPS médios, 25.000 TPS de pico e geração de alertas em no máximo 500 ms após o recebimento do evento não foram medidas porque o ensaio dedicado de desempenho não foi executado. Nenhum número de capacidade deve ser inferido do smoke ou dos testes.

A hipótese de dimensionamento é horizontal: cada partição é processada por uma tarefa ativa, e o
paralelismo máximo de um consumer group fica limitado pelo número de partições. O dimensionamento
precisa relacionar `TPS por tarefa x tarefas ativas`, considerando custo por quantidade/complexidade
das regras, distribuição de `customerId`, escrita em changelog, tamanho do histórico, disco e tempo de
restauração. As 12 partições locais demonstram o modelo, mas não são uma recomendação produtiva.

Um ensaio de aceitação deve usar payloads e regras representativos, aquecimento e duração definidos,
medir taxa de entrada/saída, lag, erros, p50/p95/p99/p99.9 e o tempo entre recebimento e publicação
durável do alerta. Também deve exercitar pico, chave quente, rebalanceamento e restauração de estado.
A hipótese só pode ser aceita se sustentar 8.000 TPS, absorver o pico de 25.000 TPS e manter todos os
alertas dentro de 500 ms no cenário exigido; caso contrário, partições, recursos ou desenho devem ser
revistos.

## MVP executável x arquitetura de produção

| Capacidade | MVP executável | Arquitetura de produção |
|---|---|---|
| broker | Kafka único, KRaft, replicação 1 | MSK multi-AZ, TLS, IAM e quotas |
| compute | três containers Spring Boot | ECS/EKS com autoscaling e identidades de workload |
| regras | JWT local, quatro olhos, snapshot/outbox | IdP corporativo, break-glass e políticas operacionais |
| estado | RocksDB + changelog | disco dimensionado, restauração ensaiada, capacidade por partição |
| banco | PostgreSQL único, schemas/usuários separados | Aurora/RDS HA, backup, KMS e rotação |
| contratos | JSON Schema versionado no repositório | Glue Schema Registry e governança de compatibilidade |
| notificação | fixture + Mailpit, retry explícito | provedor real, lease, idempotency key, circuit breaker e reconciliação |
| observabilidade | Actuator, Prometheus, Grafana e Kafka UI | OTel/CloudWatch, alertas, SLOs e retenção |
| quarentena | referência sanitizada | arquivo criptografado, acesso restrito e replay auditado |
| replay/backtest | ausente | `runId`, intervalo delimitado e notificações bloqueadas |
| capacidade | não medida | ensaios representativos antes de comprometer SLO |
| entrega automatizada | fora do escopo atual | pipeline controlado com gates definidos pela organização |

## Trade-offs

- **PostgreSQL sobre MongoDB/DynamoDB:** JSONB preserva flexibilidade da DSL, enquanto transações, constraints, locks e outbox sustentam invariantes entre regras, auditoria e snapshots.
- **Kafka sobre persistência síncrona das avaliações:** mantém o motor desacoplado e torna as saídas parte da transação do stream; consultas ad hoc exigiriam um projetor futuro.
- **Detecção assíncrona sobre motor bloqueador:** isola disponibilidade e latência da autorização, ao custo de não decidir a transação em linha.
- **Kafka Streams sobre Flink ou consumidor convencional:** entrega estado particionado, changelog e transação Kafka com menor superfície operacional para o MVP; Flink ganha quando event-time e pipelines mais complexos justificarem a plataforma.
- **DSL segura sobre Drools:** a AST tipada limita expressividade, mas evita scripts/ações arbitrárias, simplifica validação e melhora explicabilidade.
- **Snapshot completo sobre deltas:** troca atômica e avaliação reproduzível são simples; snapshots maiores exigem limite de tamanho e publicação ordenada.

## Testes e rastreabilidade

Testes unitários cobrem contratos e domínio; `TopologyTestDriver` cobre a topologia e stores; Testcontainers cobre Kafka, PostgreSQL e Mailpit; `scripts/smoke.sh` comprova a fatia vertical. A estratégia e seus limites estão em [../testing/strategy.md](../testing/strategy.md), e os ciclos registrados em [../testing/tdd-evidence.md](../testing/tdd-evidence.md).

Limitações e evoluções consolidadas: [../limitations-and-evolution.md](../limitations-and-evolution.md). Uso de IA e classes autorais: [../ai-usage.md](../ai-usage.md).
