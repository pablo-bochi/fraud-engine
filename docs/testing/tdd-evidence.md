# Evidências de TDD

## U1 — Fundação, contratos e infraestrutura

Os ciclos foram executados verticalmente com Java 21. Cada vermelho abaixo ocorreu antes da implementação correspondente.

| Comportamento | Vermelho observado | Verde observado |
|---|---|---|
| Exemplo canônico de transação é aceito | schema não encontrado | 1 teste passou após introduzir o schema |
| Campos obrigatórios são exigidos | objeto vazio não produziu violações | 2 testes passaram após declarar os campos obrigatórios |
| Valores inválidos são rejeitados | moeda, valor, ID e versão inválidos foram aceitos | 3 testes passaram após introduzir os limites |
| Todos os contratos possuem exemplo válido | exemplo versionado não encontrado | 4 testes passaram com 9 schemas e 9 exemplos |
| `TransactionEvent` faz round-trip Java/JSON | record e validador não compilavam; depois `Instant` saiu numérico | round-trip passou com `Instant` textual |
| Todos os records de transporte fazem round-trip | sete records não existiam | 2 testes de compatibilidade passaram |
| Consumidor anterior tolera extensão opcional declarada | `merchantCategory` foi rejeitado pelo schema | extensão validada e campos anteriores lidos |
| Inicialização de tópicos é repetível | política `compact,delete` quebrou a segunda execução | os 9 tópicos foram reconciliados |
| Broker local confirma transações | `InitProducerId` expirou porque a rede interna não publicava a porta | produtor confirmou e consumidor `read_committed` observou o registro |

Comandos de verificação:

```bash
./mvnw spotless:check verify -DskipITs
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.example up -d --wait
docker compose --env-file .env.example run --rm kafka-init
./mvnw verify -Pintegration
```

## U2 — Plano de controle governado das regras

| Comportamento | Vermelho observado | Verde observado |
|---|---|---|
| Proposta nasce pendente e imutável | `RuleVersion` ainda não existia | proposta preserva regra, autor e `PENDING_APPROVAL` |
| Outra pessoa aprova; autor não se autoaprova | aprovação aceitava o próprio autor | transições aprovadas/rejeitadas são imutáveis e autoaprovação é negada |
| JWT estabelece identidade administrativa confiável | emissor, audiência, `sub` vazio e expiração ausente eram aceitos | assinatura RSA, `iss`, `aud`, `sub` e `exp` são exigidos |
| Permissão REST restringe leitura de regras | token só com `RULE_WRITE` recebia 200 | mesmo token recebe 403 sem `RULE_READ` |
| Criação, alteração e retirada usam o sujeito do JWT | rotas de criação de versão não existiam | respostas `201` usam somente o `sub`; campos administrativos forjados recebem `400` |
| Aprovação e conflito possuem respostas HTTP observáveis | aprovação devolvia `200`; versão decidida devolvia `422` | aprovação devolve `202`; pendência duplicada e versão decidida devolvem `409` |
| Rejeição preserva a trilha de auditoria | exceção de auto-rejeição reverteria a auditoria | a proposta permanece pendente e a negação é gravada antes de `422` |
| Regra pendente, auditoria e regra estável persistem juntas | repositório ausente | PostgreSQL real confirma as três escritas em uma transação |
| Aprovação gera snapshot e outbox | `approve` não existia | snapshot não vazio, head, auditoria e outbox pendente são persistidos juntos |
| Autoaprovação e retirada final preservam o estado | casos não cobertos | negações são auditadas sem alterar snapshot nem criar outbox |
| Falha da primeira publicação bloqueia a seguinte | exceção escapava do relay | tentativa e backoff são registrados e N+1 permanece bloqueada |
| Falha após confirmação do Kafka | a tentativa seguinte não tinha prova do mesmo payload | a outbox mantém o evento pendente e republica o snapshot idêntico |
| Snapshot alterado na outbox | relay confiava no payload persistido | schema, versão, forma canônica e hash são verificados antes do envio |
| Limites publicáveis | candidato com muitas regras ou payload grande não era exercitado | 101 regras e payload acima de 1 MiB não promovem estado |
| Retirada válida e falha de persistência | não havia prova de proveniência ou rollback | retirada preserva a origem do snapshot e violação da outbox reverte promoção inteira |

Comandos de verificação executados nesta etapa:

```bash
./mvnw -pl services/fraud-control-service -am test
./mvnw -pl services/fraud-control-service -am verify -Pintegration
./mvnw spotless:check verify -DskipITs
# requer o Kafka local já iniciado; ativa o teste do adaptador Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 ./mvnw -pl services/fraud-control-service -am verify -Pintegration
```

## U4 — Motor de deteccao com Kafka Streams

| Comportamento | Vermelho observado | Verde observado |
|---|---|---|
| Evento normal produz somente assessment | topologia e roteamento ainda nao existiam | `NOT_SUSPICIOUS` e nenhuma saida condicional |
| Multiplas regras acionadas geram alerta consolidado | avaliacao nao consolidava o conjunto completo | regras `HIGH` e `CRITICAL` aparecem no mesmo alerta com severidade `CRITICAL` |
| Ruleset e atualizado sem redeploy | ruleset era estatico no processamento | Global Store aceita snapshot novo e novas avaliacoes usam sua versao |
| DSL composta reutiliza o dominio | topologia possuia logica de avaliacao duplicada | `ALL` e `ANY` usam o `RuleEvaluator` da U3 |
| `COUNT_WINDOW` respeita a janela declarada | janela estava fixa no adaptador | cada regra usa seu proprio `windowSeconds` |
| Historico insuficiente produz resultado inconclusivo | evento antigo era tratado como historico vazio conhecido | regra stateful retorna `NOT_EVALUATED` e assessment fica `INCONCLUSIVE` |
| Evento fora de ordem usa somente fatos anteriores | historico temporal nao distinguia adequadamente event-time | fatos posteriores ao `occurredAt` nao participam e eventos tardios contribuem para eventos futuros |
| Evento fora de ordem e identificado | `late` era sempre `false` | evento abaixo do stream-time e avaliado com `late=true` |
| Duplicata identica nao repete efeitos | deduplicacao armazenava apenas existencia do `eventId` | fingerprint igual suprime nova avaliacao |
| Mesmo `eventId` com payload diferente e isolado | replay conflitante era descartado como duplicata | conflito gera `EVENT_IDENTITY_CONFLICT` em quarentena |
| Mesmo `transactionId` com outro `eventId` e isolado | identidade conflitante nao estava protegida integralmente | conflito gera `TRANSACTION_IDENTITY_CONFLICT` sem alterar historico |
| Horizonte online de identidade expira | stores de identidade nunca expiravam logicamente | identidade e deduplicacao sao tratadas como expiradas apos 24 horas de stream-time |
| Snapshot com DSL invalida nao substitui o ativo | compiler aceitava apenas validacoes parciais | DSL nao suportada preserva o ultimo snapshot valido |
| Snapshot com hash invalido nao substitui o ativo | `contentHash` nao era verificado | SHA-256 canonico e conferido antes da ativacao |
| Snapshot fora do contrato nao substitui o ativo | schema nao era validado no motor | JSON Schema Draft-07 e aplicado antes do compile |
| Evento estruturalmente invalido nao entra no motor | Jackson aceitava valores que violavam o contrato | JSON Schema rejeita o evento, publica referencia invalida e nao altera o historico |
| Evento invalido nao contamina event-time | timestamp extractor usava `occurredAt` de payload que violava o contrato | somente payload estruturalmente valido pode avancar o stream-time |
| Kafka real confirma processamento transacional | fixture de integracao era rejeitado pelo novo contrato | Testcontainers confirma startup, ruleset valido e assessment visivel com `read_committed` |

Comandos de verificacao executados nesta etapa:

```bash
./mvnw -pl services/detection-engine -am test
./mvnw -pl services/detection-engine -am verify
./mvnw spotless:check
./mvnw clean verify

```

## U5 — Notificação idempotente e fumaça ponta a ponta

| Comportamento | Vermelho observado | Verde observado |
|---|---|---|
| Nova solicitação envia uma única notificação | handler e portas ainda não existiam | contato é resolvido, canal é chamado uma vez e resultado `SENT` é produzido |
| Solicitação já enviada é idempotente | replay ainda tentava passar pelo fluxo de entrega | `AlreadySent` reutiliza o resultado persistido sem resolver contato nem chamar SMTP |
| Falha de canal é observável e repetível | persistência não suportava `FAILED` | falha registra código sanitizado e nova tentativa usa a mesma identidade com `attempt_count` incrementado |
| Falha de cadastro não chama canal | resolução de contato não possuía caminho de falha | entrega termina em `FAILED` sem executar SMTP |
| Claims concorrentes não duplicam envio | dois consumidores poderiam competir pela mesma solicitação | PostgreSQL concede um único `PENDING/FAILED -> SENDING`; o outro observa `InProgress` |
| Consumo Kafka publica resultado | listener Kafka ainda não existia | request com chave canônica é consumido e `NotificationResult` é publicado |
| Replay com infraestrutura real não duplica SMTP | idempotência estava coberta somente por doubles | Kafka, PostgreSQL e Mailpit confirmam dois resultados iguais, uma row e uma mensagem |
| Falha após persistir `SENT` não exige novo SMTP | publisher estava acoplado diretamente ao consumer e a fronteira não era simulável | `NotificationResultPublisher` permite provar falha de publicação seguida de replay do mesmo `SENT` sem segunda entrega |
| Falha do listener não é silenciosamente descartada | política padrão de recuperação do listener podia esgotar retries | `CommonContainerStoppingErrorHandler` interrompe o container para preservar replay do record |
| Contrato de notificação é validado em runtime | Jackson permissivo aceitava campo extra `email` | JSON Schema Draft-07 rejeita contato, canal ou extensão não declarada antes da desserialização |
| Contato permanece dentro do notifier | fronteira externa ainda não estava exercitada com SMTP real | fixture resolve o e-mail somente no adapter e Mailpit é a única superfície que o contém |
| Smoke comprova a fatia vertical | não havia execução completa dos três serviços | regra aprovada produz assessment normal e suspeito, alerta, request, `SENT` e exatamente um e-mail mesmo após replay |

Comandos de verificação executados nesta etapa:

```bash
./mvnw -pl services/notification-service -am test
./mvnw -pl services/notification-service -am verify -Pintegration
docker compose --env-file .env.example config --quiet
./scripts/smoke.sh
```
