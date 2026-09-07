# Declaração de uso de IA

## Escopo

IA foi usada como apoio para refinamento do plano, explicação de alternativas, geração e revisão de partes da implementação, investigação de falhas, testes e documentação. As decisões de produto e arquitetura registradas no plano canônico foram aprovadas pelo autor do case, que permanece responsável por compreender, validar e defender o comportamento entregue.

Não se atribui à IA autoria autônoma da solução nem se usa geração como substituto para execução de testes. As afirmações desta documentação foram confrontadas com o repositório e com verificações locais.

## Classes implementadas integralmente pelo autor

As duas classes reservadas antes da U3 e implementadas integralmente por Pablo Bochi foram:

- `services/detection-engine/src/main/java/com/fraudengine/detection/application/DeterministicIdFactory.java` — deriva `assessmentId`, `alertId` e `notificationRequestId` em namespaces separados e de forma determinística;
- `services/detection-engine/src/main/java/com/fraudengine/detection/application/AssessmentAggregator.java` — aplica a tabela de decisão `SUSPICIOUS` / `INCONCLUSIVE` / `NOT_SUSPICIOUS` e seleciona a maior severidade entre regras acionadas.

Para essas classes, a IA pôde esclarecer requisitos e revisar o resultado depois de apresentado, mas não gerou sua implementação. Essa reserva está registrada na nota de execução da U3 do plano canônico.

## Responsabilidades humanas

O autor é responsável por:

- decisões de escopo, inclusive detecção assíncrona, quatro olhos e a exclusão de CI na U9;
- escolha e domínio das duas classes autorais;
- revisão dos contratos, limites, riscos e trade-offs;
- execução e interpretação dos testes;
- qualquer afirmação futura de capacidade, segurança ou prontidão produtiva.

## Limites da declaração

O histórico Git identifica o autor dos commits, mas não é, sozinho, prova de como cada linha foi produzida. Esta declaração usa o compromisso e a reserva explícitos no plano para as duas classes acima e evita percentuais artificiais de contribuição.
