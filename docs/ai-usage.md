# Declaração de uso de IA

## Escopo

IA foi usada como apoio para refinamento do plano, explicação de alternativas, geração e revisão de partes da implementação, investigação de falhas, testes e documentação. As decisões de produto e arquitetura registradas no plano canônico foram aprovadas pelo autor do case, que permanece responsável por compreender, validar e defender o comportamento entregue.

Não se atribui à IA autoria autônoma da solução nem se usa geração como substituto para execução de testes. As afirmações desta documentação foram confrontadas com o repositório e com verificações locais.

## Como e quando a IA foi usada

| Momento | Apoio da IA | Validação humana |
|---|---|---|
| entendimento e planejamento | organizar requisitos, explorar alternativas e estruturar a sequência de entrega | escolhas de escopo, arquitetura e trade-offs foram avaliadas e aprovadas pelo autor |
| implementação | sugerir código, testes e ajustes em componentes não reservados ao autor | cada mudança foi revisada contra contratos, invariantes e comportamento esperado |
| investigação e verificação | interpretar falhas de build, integração e smoke; propor hipóteses e testes de regressão | hipóteses foram confirmadas por execução local, não aceitas apenas pela resposta gerada |
| documentação final | cruzar README, arquitetura, configurações, testes, histórico e requisitos do case | afirmações, limitações e comandos foram conferidos no repositório e no ambiente local |

A IA não foi usada para produzir resultados de capacidade: vazão e latência permanecem declaradas
como não medidas. Também não substituiu decisões jurídicas ou operacionais sobre LGPD, segurança e
prontidão produtiva.

## Classes implementadas integralmente pelo autor

As duas classes reservadas antes da implementação do domínio de detecção e desenvolvidas integralmente por Pablo Bochi foram:

- `services/detection-engine/src/main/java/com/fraudengine/detection/application/DeterministicIdFactory.java` — deriva `assessmentId`, `alertId` e `notificationRequestId` em namespaces separados e de forma determinística;
- `services/detection-engine/src/main/java/com/fraudengine/detection/application/AssessmentAggregator.java` — aplica a tabela de decisão `SUSPICIOUS` / `INCONCLUSIVE` / `NOT_SUSPICIOUS` e seleciona a maior severidade entre regras acionadas.

Para essas classes, a IA pôde esclarecer requisitos e revisar o resultado depois de apresentado, mas não gerou sua implementação. Essa reserva foi definida antes do desenvolvimento e está registrada no plano canônico.

## Responsabilidades humanas

O autor é responsável por:

- decisões de escopo, inclusive detecção assíncrona, aprovação por quatro olhos e a exclusão de CI da entrega documental;
- escolha e domínio das duas classes autorais;
- revisão dos contratos, limites, riscos e trade-offs;
- execução e interpretação dos testes;
- qualquer afirmação futura de capacidade, segurança ou prontidão produtiva.

## Limites da declaração

O histórico Git identifica o autor dos commits, mas não é, sozinho, prova de como cada linha foi produzida. Esta declaração usa o compromisso e a reserva explícitos no plano para as duas classes acima e evita percentuais artificiais de contribuição.
