package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.contracts.schema.SchemaValidator;
import java.io.IOException;

final class TransactionEventPayloadValidator {

  private static final String CONTRACT_NAME = "transaction-event-v1";

  private final ObjectMapper mapper =
      new ObjectMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final SchemaValidator schemaValidator = SchemaValidator.draft7();

  TransactionEvent parseValid(byte[] payload) {
    if (payload == null) {
      return null;
    }

    try {
      JsonNode document = mapper.readTree(payload);

      if (document == null
          || !document.isObject()
          || !schemaValidator.validate(CONTRACT_NAME, document).isEmpty()) {
        return null;
      }

      return mapper.treeToValue(document, TransactionEvent.class);
    } catch (IOException | IllegalArgumentException exception) {
      return null;
    }
  }
}
