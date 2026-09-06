package com.fraudengine.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ContractSchemaTest {

  private static final List<String> CONTRACTS =
      List.of(
          "transaction-event-v1",
          "transaction-assessment-v1",
          "internal-alert-v1",
          "customer-notification-requested-v1",
          "notification-result-v1",
          "ruleset-snapshot-v1",
          "investigation-feedback-v1",
          "invalid-event-reference-v1",
          "quarantined-event-reference-v1");

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void validTransactionEventConformsToItsDeclaredSchema() throws Exception {
    try (InputStream schemaStream =
        getClass().getResourceAsStream("/schemas/transaction-event-v1.schema.json")) {
      assertThat(schemaStream).as("transaction event schema").isNotNull();

      JsonSchema schema =
          JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaStream);
      JsonNode event =
          objectMapper.readTree(
              """
              {
                "schemaVersion": 1,
                "eventId": "evt-01JY9VX8E8Z4X6K2W3P1Q7R5T9",
                "transactionId": "txn-01JY9VY0AZ3G6N8H4M2K7C5D1F",
                "customerId": "cus-01JY9VY9KQ8B3E7T5R2W6M4N0P",
                "amountMinor": 12500,
                "currency": "BRL",
                "occurredAt": "2026-09-05T12:30:00Z",
                "transactionType": "PURCHASE",
                "channel": "ECOMMERCE",
                "merchantCountry": "BR",
                "deviceIdHash": "sha256:7e31f0f8cfd4b26f",
                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736"
              }
              """);

      assertThat(schema.validate(event)).isEmpty();
    }
  }

  @Test
  void transactionEventWithoutRequiredFieldsIsRejected() throws Exception {
    try (InputStream schemaStream =
        getClass().getResourceAsStream("/schemas/transaction-event-v1.schema.json")) {
      assertThat(schemaStream).as("transaction event schema").isNotNull();

      JsonSchema schema =
          JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaStream);

      assertThat(schema.validate(objectMapper.createObjectNode())).isNotEmpty();
    }
  }

  @Test
  void transactionEventWithInvalidValuesIsRejected() throws Exception {
    try (InputStream schemaStream =
        getClass().getResourceAsStream("/schemas/transaction-event-v1.schema.json")) {
      assertThat(schemaStream).as("transaction event schema").isNotNull();
      JsonSchema schema =
          JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaStream);

      ObjectNode invalidCurrency = validTransactionEvent().put("currency", "REAL");
      ObjectNode negativeAmount = validTransactionEvent().put("amountMinor", -1);
      ObjectNode oversizedId = validTransactionEvent().put("eventId", "x".repeat(129));
      ObjectNode unsupportedVersion = validTransactionEvent().put("schemaVersion", 2);

      assertThat(List.of(invalidCurrency, negativeAmount, oversizedId, unsupportedVersion))
          .allSatisfy(event -> assertThat(schema.validate(event)).isNotEmpty());
    }
  }

  @Test
  void everyCanonicalContractProvidesAValidVersionedExample() throws Exception {
    for (String contract : CONTRACTS) {
      try (InputStream schemaStream =
              getClass().getResourceAsStream("/schemas/" + contract + ".schema.json");
          InputStream exampleStream =
              getClass().getResourceAsStream("/examples/valid/" + contract + ".json")) {
        assertThat(schemaStream).as("schema for %s", contract).isNotNull();
        assertThat(exampleStream).as("valid example for %s", contract).isNotNull();
        JsonSchema schema =
            JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaStream);
        assertThat(schema.validate(objectMapper.readTree(exampleStream)))
            .as("violations for %s", contract)
            .isEmpty();
      }
    }
  }

  @Test
  void versionedInvalidExamplesAreRejectedByTheirDeclaredSchemas() throws Exception {
    Map<String, String> examples =
        Map.of(
            "transaction-event-missing-required-v1", "transaction-event-v1",
            "transaction-event-invalid-currency-v1", "transaction-event-v1",
            "transaction-event-negative-amount-v1", "transaction-event-v1",
            "transaction-event-unsupported-version-v1", "transaction-event-v1",
            "customer-notification-with-channel-v1", "customer-notification-requested-v1",
            "ruleset-snapshot-empty-v1", "ruleset-snapshot-v1");

    for (Map.Entry<String, String> example : examples.entrySet()) {
      try (InputStream invalid =
          getClass().getResourceAsStream("/examples/invalid/" + example.getKey() + ".json")) {
        assertThat(invalid).as("invalid example %s", example.getKey()).isNotNull();
        assertThat(
                com.fraudengine.contracts.schema.SchemaValidator.draft7()
                    .validate(example.getValue(), objectMapper.readTree(invalid)))
            .as("violations for %s", example.getKey())
            .isNotEmpty();
      }
    }
  }

  private ObjectNode validTransactionEvent() throws Exception {
    return (ObjectNode)
        objectMapper.readTree(
            """
            {
              "schemaVersion": 1,
              "eventId": "evt-01JY9VX8E8Z4X6K2W3P1Q7R5T9",
              "transactionId": "txn-01JY9VY0AZ3G6N8H4M2K7C5D1F",
              "customerId": "cus-01JY9VY9KQ8B3E7T5R2W6M4N0P",
              "amountMinor": 12500,
              "currency": "BRL",
              "occurredAt": "2026-09-05T12:30:00Z",
              "transactionType": "PURCHASE",
              "channel": "ECOMMERCE",
              "merchantCountry": "BR",
              "deviceIdHash": "sha256:7e31f0f8cfd4b26f",
              "traceId": "4bf92f3577b34da6a3ce929d0e0e4736"
            }
            """);
  }
}
