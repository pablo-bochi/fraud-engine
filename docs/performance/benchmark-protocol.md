# Protocolo do benchmark de capacidade

## Objetivo e limites

Este ensaio verifica, em uma unica maquina, se a implementacao local sustenta 8.000 TPS, absorve um pico de 25.000 TPS e torna 99,9% das avaliacoes e 99,9% dos alertas internos visiveis a um consumidor `read_committed` em ate 500 ms depois que o envio e aceito pelo produtor da carga.

O resultado caracteriza somente o hardware, os containers e a configuracao registrados. Ele nao certifica capacidade produtiva, alta disponibilidade, recuperacao de estado ou dimensionamento horizontal.

## Preparacao reproduzivel

Use Python 3.11 ou posterior, Docker e o JAR do motor produzido pelo build do repositorio:

```bash
./mvnw package -DskipTests
python3 -m venv /tmp/fraud-engine-u8-venv
/tmp/fraud-engine-u8-venv/bin/python -m pip install \
  -r tools/load-test/requirements.txt

docker compose \
  --project-name fraud-engine-u8 \
  --env-file .env.example \
  down -v --remove-orphans

docker compose \
  --project-name fraud-engine-u8 \
  --env-file .env.example \
  up --build -d kafka kafka-init detection-engine
```

Somente Kafka, o inicializador de topicos e o motor participam da medicao. O servico de notificacao fica fora para que SMTP e PostgreSQL nao alterem a medida das saidas Kafka do motor. A remocao previa dos volumes evita misturar backlog, ruleset ou stores de outra execucao. A topologia medida inclui o reparticionamento por `customerId` e seus changelogs.

## Execucao

Primeiro valide a ferramenta e uma carga curta:

```bash
/tmp/fraud-engine-u8-venv/bin/python -m pytest \
  tools/load-test/test_load_test.py -q

/tmp/fraud-engine-u8-venv/bin/python tools/load-test/load_test.py \
  --bootstrap-servers 127.0.0.1:9094 \
  --compose-project fraud-engine-u8 \
  --publish-ruleset \
  --scenario low-rate:100:2 \
  --timeout 60 \
  --report /tmp/fraud-engine-u8-low-rate.md
```

A carga completa usa semente fixa, 12 particoes, uma thread do Kafka Streams, `exactly_once_v2` e um alerta a cada 1.000 entradas:

```bash
/tmp/fraud-engine-u8-venv/bin/python tools/load-test/load_test.py \
  --bootstrap-servers 127.0.0.1:9094 \
  --compose-project fraud-engine-u8 \
  --publish-ruleset \
  --timeout 600 \
  --report docs/performance/results.md
```

Os cenarios padrao sao aquecimento de 1.000 TPS por 5 s, carga sustentada de 8.000 TPS por 30 s e pico de 25.000 TPS por 5 s. `--scenario NOME:TPS:SEGUNDOS` permite uma execucao deliberadamente diferente; o relatorio sempre registra os valores usados.

Ao terminar:

```bash
docker compose \
  --project-name fraud-engine-u8 \
  --env-file .env.example \
  down -v --remove-orphans
```

## Geracao e reconciliacao

- Cada evento recebe `eventId`, `transactionId` e `traceId` unicos; a distribuicao de clientes e valores usa a semente `20260907`.
- O produtor usa `acks=all`, idempotencia, lote e compressao LZ4. A taxa e controlada pelo relogio monotonicamente crescente do host.
- Um ruleset minimo de limite monetario e publicado antes da carga, e a ferramenta espera `streamsReadiness.loadedVersion > 0`. Isso evita o risco de bootstrap sem regras neste roteiro, mas nao demonstra que o motor pause o consumo quando o ruleset esta ausente.
- O consumidor inicia no fim dos topicos de avaliacao e alerta, usa `isolation.level=read_committed` e roda numa thread dedicada, desacoplada do ritmo do produtor. Ele drena ate 1.000 mensagens por chamada, e o instante de observacao do lote e capturado antes da desserializacao.
- Toda entrada deve possuir exatamente uma avaliacao. Toda avaliacao `SUSPICIOUS` deve possuir exatamente um alerta, e nenhuma outra avaliacao pode possuir alerta.
- Qualquer perda, saida inesperada ou duplicacao encerra o processo com codigo diferente de zero. O relatorio final so e gravado depois da reconciliacao integral.

## Medidas e criterio

O criterio principal e o intervalo monotono entre o envio aceito pelo produtor e a observacao `read_committed` da saida. Ele inclui fila do produtor, topico de entrada, reparticionamento, espera da tarefa, avaliacao, commit transacional, topico de saida e atraso do observador. Como a observacao acontece depois de a saida estar duravel, a medida e um limite superior conservador do caminho ponta a ponta deste ensaio.

A ferramenta calcula distribuicoes separadas para avaliacoes e alertas. Ambas precisam ter pelo menos 99,9% das amostras em ate 500 ms. `readCommittedObservedAt - evaluatedAt` e `readCommittedObservedAt - alertCreatedAt` sao exibidas apenas para diagnosticar o trecho posterior a avaliacao; elas nao decidem a meta porque nao incluem a espera de entrada nem a propria avaliacao.

A vazao observada divide o total reconciliado pelo intervalo entre o primeiro envio e a ultima avaliacao ou alerta esperado. A meta de vazao exige que essa taxa alcance o alvo do cenario. Sao registrados p50, p95, p99, p99,9, maximo, maior lag amostrado, perdas e duplicacoes. Se a reconciliacao falhar ou expirar, os percentis parciais sao considerados censurados e nao podem ser apresentados como resultado do cenario.

Os numeros medidos nesta sessao estao em [results.md](results.md).
