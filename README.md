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
