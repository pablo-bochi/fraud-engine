package com.fraudengine.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.schema.SchemaValidator;
import java.io.InputStream;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ContractCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

  @Test
  void transactionEventRoundTripsThroughItsJavaContract() throws Exception {
    try (InputStream example =
        getClass().getResourceAsStream("/examples/valid/transaction-event-v1.json")) {
      assertThat(example).isNotNull();
      TransactionEvent event = objectMapper.readValue(example, TransactionEvent.class);

      JsonNode serialized = objectMapper.valueToTree(event);

      assertThat(event.amountMinor()).isEqualTo(12_500L);
      assertThat(SchemaValidator.draft7().validate("transaction-event-v1", serialized)).isEmpty();
    }
  }

  @Test
  void everyTransportRecordRoundTripsThroughItsCanonicalSchema() throws Exception {
    Map<String, Class<?>> contracts =
        Map.of(
            "transaction-event-v1", TransactionEvent.class,
            "transaction-assessment-v1", TransactionAssessment.class,
            "internal-alert-v1", InternalAlert.class,
            "customer-notification-requested-v1", CustomerNotificationRequested.class,
            "notification-result-v1", NotificationResult.class,
            "ruleset-snapshot-v1", RuleSetSnapshot.class,
            "invalid-event-reference-v1", InvalidEventReference.class,
            "quarantined-event-reference-v1", QuarantinedEventReference.class);

    for (Map.Entry<String, Class<?>> contract : contracts.entrySet()) {
      try (InputStream example =
          getClass().getResourceAsStream("/examples/valid/" + contract.getKey() + ".json")) {
        Object value = objectMapper.readValue(example, contract.getValue());
        assertThat(
                SchemaValidator.draft7()
                    .validate(contract.getKey(), objectMapper.valueToTree(value)))
            .as("round-trip violations for %s", contract.getKey())
            .isEmpty();
      }
    }
  }

  @Test
  void previousTransactionConsumerReadsADeclaredOptionalField() throws Exception {
    try (InputStream example =
        getClass().getResourceAsStream("/examples/valid/transaction-event-v1.json")) {
      ObjectNode extended = (ObjectNode) objectMapper.readTree(example);
      extended.put("merchantCategory", "5734");

      assertThat(SchemaValidator.draft7().validate("transaction-event-v1", extended)).isEmpty();
      TransactionEvent consumed = objectMapper.treeToValue(extended, TransactionEvent.class);
      assertThat(consumed.transactionId()).isEqualTo("txn-01");
    }
  }
}
