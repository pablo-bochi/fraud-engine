# Limitações e evolução

## Estado da entrega

- U1–U5 estão implementadas e formam o fluxo obrigatório.
- U6 opcional está implementada com Prometheus, Grafana e Kafka UI.
- U7 foi absorvida pelo smoke de U5; não é pendência separada.
- O benchmark opcional de capacidade foi executado; a ferramenta, o protocolo e os resultados medidos estão em `tools/load-test/` e `docs/performance/`.
- U9 documenta o repositório. Automação de CI foi explicitamente retirada de seu escopo; a verificação é local e reproduzível.

## Limitações do MVP

### Consistência e streaming

- Um único broker local usa fator de replicação 1; isso não prova tolerância a falhas.
- `exactly_once_v2` cobre a transação Kafka, não o sistema inteiro nem SMTP.
- A deduplicação global de `eventId` entre clientes não existe. Após o reparticionamento, o store é por cliente.
- Identidade online expira em 24 horas de stream-time; replay posterior pode voltar a ser processado.
- O histórico de regras stateful é retido por 15 minutos. Não há watermark, espera por reordenação ou correção retroativa.
- O motor não bloqueia o consumo até conhecer um end offset do tópico de ruleset. Prontidão exige snapshot carregado, mas não prova convergência global.
- Não há coordenação entre múltiplas instâncias para ativação simultânea de uma versão.

### Regras e operação

- A DSL suporta apenas `AMOUNT_THRESHOLD`, `COUNT_WINDOW`, `ALL` e `ANY`.
- Não há retirada emergencial que ultrapasse versões pendentes nem coalescimento de snapshots.
- A outbox possui retry ordenado e backoff, mas não oferece console operacional, lease distribuído ou replay autorizado.
- O tópico de DLQ de notificação é provisionado, porém não há produtor automático para ele.
- Quarentena e inválidos contêm referências sanitizadas; investigação, payload protegido e reexecução não são executáveis.

### Notificação

- Contatos são fixtures e o canal é Mailpit; não há provedor externo.
- Uma linha abandonada em `SENDING` não é recuperada automaticamente.
- O intervalo entre aceite SMTP e persistência de `SENT` não é reconciliado.
- Não há idempotency key no provedor, supressão, circuit breaker ou DLQ automática.

### Segurança e privacidade

- JWTs são emitidos por script local e duram uma hora; não há IdP, rotação ou revogação.
- Kafka local usa PLAINTEXT e o banco usa segredos demonstrativos.
- Actuator/Prometheus estão sem autenticação na rede local do Compose.
- Não há MSK IAM/TLS, KMS, Secrets Manager, WAF, arquivo criptografado ou política automatizada de retenção/descarte.
- A demonstração minimiza payloads e separa contato, mas não constitui uma avaliação jurídica completa de LGPD.

### Escala e entrega

- As metas de 8.000 TPS, pico de 25.000 TPS e 99,9% em até 500 ms foram testadas no MacBook Air M1 com 4,12 GB atribuídos ao Docker, 12 partições e uma thread do Kafka Streams. A meta de latência foi atingida nos dois cenários-alvo, com 100% das amostras em até 500 ms; as metas de vazão não foram atingidas: 5.717,37 TPS na carga sustentada e 5.749,18 TPS no pico. Todas as 365.000 entradas dos dois cenários-alvo foram reconciliadas, sem perda ou duplicação. Consulte `docs/performance/results.md`; os números não são certificação produtiva.
- Não há tratamento de chaves quentes por salting, múltiplos fluxos ou dimensionamento automático.
- O dashboard local demonstra sinais, mas não define SLO, paging ou retenção produtiva.
- Não há automação de CI nesta entrega por decisão de escopo.

## Evolução recomendada

1. Repetir o benchmark de capacidade com mais recursos, mais threads/instâncias e perfis de chaves, documentando cada configuração e mantendo a reconciliação antes de discutir SLO.
2. Ensaiar restauração de changelogs, indisponibilidade e múltiplas instâncias; medir tempo de convergência e tamanho dos stores.
3. Adicionar IdP corporativo, MSK IAM/TLS, Secrets Manager/KMS, ACLs por tópico e identidade de workload.
4. Criar arquivo de entrada criptografado, investigação com acesso restrito e replay auditado com notificações bloqueadas.
5. Fechar a fronteira SMTP com lease, recuperação de `SENDING`, idempotência do provedor, reconciliação, backoff e circuit breaker.
6. Projetar modelos de leitura somente quando consultas reais exigirem, sem mover persistência síncrona para o caminho do motor.
7. Implementar backtest com `runId`, intervalos delimitados e namespace de IDs separado.
8. Avaliar salting/agregação em dois estágios caso métricas mostrem clientes ou partições quentes.
9. Introduzir automação de entrega quando o escopo permitir, preservando os mesmos gates locais de schema, build, testes e smoke.
