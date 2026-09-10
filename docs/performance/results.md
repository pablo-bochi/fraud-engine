# Resultados do benchmark de capacidade

Executado em `2026-09-09T23:40:02.037763+00:00` com semente fixa `20260907`.

> Este ensaio local nao certifica capacidade produtiva. Os numeros valem somente para o ambiente e a configuracao registrados abaixo.

## Ambiente

- **host:** `MacBook-Air-de-Pablo.local`
- **os:** `macOS-26.5-arm64-arm-64bit`
- **machine:** `arm64`
- **python:** `3.11.4`
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
- **output_consume_batch_size:** `1000`
- **scenario_timeout_seconds:** `600.0`
- **containers:** `fraud-engine-u8-detection-engine-1=fraud-engine-u8-detection-engine (1b19ff5fd08d); fraud-engine-u8-kafka-1=apache/kafka:4.2.0 (74d44a051e0f)`
- **ruleset:** `snapshot=70a6bf99-2103-4c73-a1ca-0e784414d256, version=1, hash=sha256:5b817609e08a5016f53d38cef68437f31b6091f922bc89db1f5e07452a191891, rule=AMOUNT_THRESHOLD(10000 BRL)`
- **alert_every:** `1000`
- **scenarios:** `warmup:1000:5s, sustained-8000:8000:30s, peak-25000:25000:5s`

## Resultados

| Cenario | Meta | Envio | Vazao observada | avaliacoes <= 500 ms | alertas <= 500 ms | Vazao | Latencia E2E |
|---|---:|---:|---:|---:|---:|---|---|
| warmup | 1.000,00 TPS | 1.000,19 TPS | 949,70 TPS | 90,48% | 80,00% | NAO ATINGIDA | NAO ATINGIDA |
| sustained-8000 | 8.000,00 TPS | 8.000,12 TPS | 5.252,34 TPS | 26,27% | 25,42% | NAO ATINGIDA | NAO ATINGIDA |
| peak-25000 | 25.000,00 TPS | 25.003,76 TPS | 2.563,37 TPS | 0,31% | 0,00% | NAO ATINGIDA | NAO ATINGIDA |

## Distribuicao de latencia

### warmup

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| avaliacao E2E: observacao `read_committed` - envio aceito pelo produtor | 262,08 ms | 593,13 ms | 693,24 ms | 755,19 ms | 777,27 ms |
| alerta E2E: observacao `read_committed` - envio aceito pelo produtor | 258,16 ms | 528,45 ms | 571,95 ms | 581,74 ms | 582,82 ms |
| diagnostico da avaliacao: observacao `read_committed` - `evaluatedAt` | 132,96 ms | 171,28 ms | 178,34 ms | 216,71 ms | 218,56 ms |
| diagnostico do alerta: observacao `read_committed` - `createdAt` | 111,22 ms | 126,21 ms | 126,47 ms | 126,52 ms | 126,53 ms |

Integridade: 5000 entradas unicas, 5000 avaliacoes, 5 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 0 registros.

### sustained-8000

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| avaliacao E2E: observacao `read_committed` - envio aceito pelo produtor | 2.910,31 ms | 14.935,42 ms | 16.885,23 ms | 18.353,66 ms | 19.481,97 ms |
| alerta E2E: observacao `read_committed` - envio aceito pelo produtor | 2.920,83 ms | 14.667,91 ms | 16.190,88 ms | 17.465,05 ms | 17.702,85 ms |
| diagnostico da avaliacao: observacao `read_committed` - `evaluatedAt` | 108,17 ms | 178,93 ms | 212,10 ms | 316,32 ms | 385,84 ms |
| diagnostico do alerta: observacao `read_committed` - `createdAt` | 118,86 ms | 183,75 ms | 216,84 ms | 239,54 ms | 243,55 ms |

Integridade: 240000 entradas unicas, 240000 avaliacoes, 240 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 572 registros.

### peak-25000

| Medida | p50 | p95 | p99 | p99,9 | maximo |
|---|---:|---:|---:|---:|---:|
| avaliacao E2E: observacao `read_committed` - envio aceito pelo produtor | 20.907,10 ms | 41.712,92 ms | 44.425,04 ms | 45.339,59 ms | 45.673,82 ms |
| alerta E2E: observacao `read_committed` - envio aceito pelo produtor | 21.478,72 ms | 41.851,75 ms | 44.058,48 ms | 44.234,25 ms | 44.249,55 ms |
| diagnostico da avaliacao: observacao `read_committed` - `evaluatedAt` | 125,47 ms | 213,97 ms | 274,70 ms | 329,84 ms | 364,06 ms |
| diagnostico do alerta: observacao `read_committed` - `createdAt` | 128,13 ms | 214,64 ms | 269,18 ms | 311,17 ms | 315,20 ms |

Integridade: 125000 entradas unicas, 125000 avaliacoes, 125 alertas, zero perdas e zero duplicacoes. Maior atraso de consumo observado: 517 registros.

## Interpretacao

A vazao observada considera o intervalo entre o primeiro envio e a observacao da ultima avaliacao/alerta esperado. A meta de latencia usa, separadamente para avaliacoes e alertas, o intervalo monotono entre o envio aceito pelo produtor e a observacao da saida por um consumidor `read_committed`. Ela inclui fila do produtor, entrada no Kafka, reparticionamento, espera da tarefa, avaliacao, commit transacional, saida no Kafka e atraso do observador; por isso e um limite superior conservador do tempo ponta a ponta deste ensaio. As medidas iniciadas em `evaluatedAt`/`createdAt` sao apenas diagnosticas e nao decidem a meta de 500 ms.
