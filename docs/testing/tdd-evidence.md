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
