# Resultados do benchmark U8

Executado em `2026-09-07T15:22:03.398543+00:00` com semente fixa `20260907`.

> Este ensaio local nao certifica capacidade produtiva. Os numeros valem somente para o ambiente e a configuracao registrados abaixo.

## Ambiente

- **host:** `MacBook-Air-de-Pablo.local`
- **os:** `macOS-26.5-arm64-arm-64bit`
- **machine:** `arm64`
- **python:** `3.12.3`
- **logical_cpus:** `8`
- **cpu:** `Apple M1`
- **memory_bytes:** `8589934592`
- **docker_server:** `24.0.5`
- **docker_memory_bytes:** `4124446720`

## Configuracao

- **bootstrap_servers:** `127.0.0.1:9094`
- **partitions:** `12`
- **stream_threads:** `1`
- **processing_guarantee:** `exactly_once_v2`
- **consumer_isolation:** `read_committed`
- **containers:** `fraud-engine-u8-detection-engine-1=fraud-engine-u8-detection-engine (9c23a67f8b86); fraud-engine-u8-kafka-1=apache/kafka:4.2.0 (0e2ea837dbfd)`
- **ruleset:** `snapshot=753b60ff-b2a0-4e23-9332-3b1cdf39ff9d, version=1, hash=sha256:fc47e7fe594a47ff820255e3fec95e9a66addad0d7c5de2da657354a9babb0bb, rule=AMOUNT_THRESHOLD(10000 BRL)`
- **alert_every:** `1000`
- **scenarios:** `warmup:1000:5s, sustained-8000:8000:30s, peak-25000:25000:5s`

## Resultados

| Cenario | Meta | Envio | Vazao observada | <= 500 ms | Vazao | Latencia |
|---|---:|---:|---:|---:|---|---|
| warmup | 1.000,00 TPS | 1.000,18 TPS | 974,26 TPS | 100,00% | NAO ATINGIDA | ATINGIDA |
| sustained-8000 | 8.000,00 TPS | 8.000,05 TPS | 5.717,37 TPS | 100,00% | NAO ATINGIDA | ATINGIDA |
| peak-25000 | 25.000,00 TPS | 25.000,76 TPS | 5.749,18 TPS | 100,00% | NAO ATINGIDA | ATINGIDA |

## Distribuicao de latencia

### warmup

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| `readCommittedObservedAt - engineReceivedAt/alertCreatedAt` | 86,11 ms | 137,69 ms | 173,04 ms | 191,98 ms | 198,96 ms |
| observacao `read_committed` - envio do produtor | 122,39 ms | 198,70 ms | 239,67 ms | 267,09 ms | 283,90 ms |

Integridade: 5000 entradas unicas, 5000 avaliacoes, 5 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 93 registros.

### sustained-8000

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| `readCommittedObservedAt - engineReceivedAt/alertCreatedAt` | 96,33 ms | 186,10 ms | 240,15 ms | 413,36 ms | 442,08 ms |
| observacao `read_committed` - envio do produtor | 6.848,08 ms | 13.537,52 ms | 14.991,38 ms | 16.117,11 ms | 16.461,03 ms |

Integridade: 240000 entradas unicas, 240000 avaliacoes, 240 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 1513 registros.

### peak-25000

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| `readCommittedObservedAt - engineReceivedAt/alertCreatedAt` | 80,44 ms | 147,41 ms | 218,90 ms | 401,76 ms | 421,56 ms |
| observacao `read_committed` - envio do produtor | 7.850,53 ms | 14.953,67 ms | 16.470,69 ms | 16.939,79 ms | 16.976,34 ms |

Integridade: 125000 entradas unicas, 125000 avaliacoes, 125 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 1464 registros.

## Interpretacao

A vazao observada considera o intervalo entre o primeiro envio e a observacao da ultima avaliacao/alerta esperado. A latencia duravel usa `receivedAt` nas avaliacoes, `createdAt` nos alertas e o instante em que o consumidor `read_committed` recebeu o registro, portanto e um limite superior da publicacao duravel. A latencia desde o produtor e apresentada separadamente.
