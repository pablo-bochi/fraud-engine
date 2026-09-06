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
