# Fraud Detection Engine

Fundação de um motor assíncrono de detecção de transações suspeitas. A unidade U1 contém os contratos canônicos versionados, os tipos Java de transporte e a infraestrutura local compartilhada pelas unidades seguintes.

## Pré-requisitos

- Java 21
- Docker com Compose v2

## Build e contratos

```bash
./mvnw spotless:check verify -DskipITs
```

Os schemas JSON Draft-07 em `contracts/schemas` são a interface externa. Exemplos aceitos e rejeitados ficam em `contracts/examples`, e o módulo `libs/contracts-java` fornece records de transporte e o `SchemaValidator`.

## Infraestrutura local

```bash
docker compose --env-file .env.example up -d --wait
docker compose --env-file .env.example run --rm kafka-init
./mvnw verify -Pintegration
```

O Compose inicia Kafka em KRaft, PostgreSQL e Mailpit. Kafka e Mailpit expõem portas somente em `127.0.0.1` para testes e demonstração; PostgreSQL permanece restrito à rede interna. O inicializador de tópicos pode ser executado novamente para reconciliar partições, retenção e compactação.

Para encerrar os processos sem remover os dados locais:

```bash
docker compose --env-file .env.example down
```

As credenciais de `.env.example` são exclusivas do ambiente local. Arquivos `.env`, estado de execução e credenciais geradas não são versionados.

## Serviço de controle de regras local

O emissor de desenvolvimento gera um par RSA e dois JWTs de curta duração sob `.local/security/`. O serviço recebe somente a chave pública para validar assinatura, emissor, audiência, expiração e permissões.

```bash
./scripts/dev-bootstrap.sh
./mvnw -pl services/fraud-control-service -am package -DskipTests
docker compose --env-file .env.example up -d --wait --build
docker compose --env-file .env.example run --rm kafka-init
```

O serviço fica disponível em `http://localhost:8080`. Os tokens ficam em `.local/security/rule-author.jwt`, `.local/security/rule-approver.jwt` e `.local/security/rule-auditor.jwt`; eles expiram em uma hora e podem ser renovados executando o bootstrap novamente.

| Método e rota | Permissão | Uso |
|---|---|---|
| `POST /api/v1/rules` | `RULE_WRITE` | Cria regra e versão inicial pendente. |
| `POST /api/v1/rules/{ruleId}/versions` | `RULE_WRITE` | Propõe `UPSERT` ou `RETIRE`. |
| `POST /api/v1/rules/{ruleId}/versions/{versionId}/approval` | `RULE_APPROVE` | Aprova e responde `202` com a publicação pendente. |
| `POST /api/v1/rules/{ruleId}/versions/{versionId}/rejection` | `RULE_APPROVE` | Rejeita a proposta. |
| `GET /api/v1/rules` | `RULE_READ` | Lista regras estáveis. |
| `GET /api/v1/rules/{ruleId}/versions` | `RULE_READ` | Lista versões de uma regra. |
| `GET /api/v1/rulesets/active` | `RULE_READ` | Exibe versões desejada e publicada, snapshot e acúmulo da outbox. |
| `GET /api/v1/rulesets/active/outbox` | `RULE_READ` | Lista tentativas e estado da outbox. |
| `GET /api/v1/audit-events` | `AUDIT_READ` | Lista a auditoria somente de acréscimo. |
