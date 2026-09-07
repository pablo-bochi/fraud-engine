package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.contracts.schema.SchemaValidator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

final class RuleSetSnapshotValidator {

  private static final String CONTRACT_NAME = "ruleset-snapshot-v1";

  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  private final SchemaValidator schemaValidator = SchemaValidator.draft7();

  void validate(RuleSetSnapshot snapshot) {
    if (snapshot == null) {
      throw new IllegalArgumentException("INVALID_RULESET_SNAPSHOT");
    }

    JsonNode document = MAPPER.valueToTree(snapshot);

    if (!schemaValidator.validate(CONTRACT_NAME, document).isEmpty()) {
      throw new IllegalArgumentException("INVALID_RULESET_SCHEMA");
    }

    validateContentHash(document);
  }

  private void validateContentHash(JsonNode document) {

    ObjectNode unsigned = ((ObjectNode) document).deepCopy();

    JsonNode declaredHashNode = unsigned.remove("contentHash");

    if (declaredHashNode == null || declaredHashNode.asText().isBlank()) {
      throw new IllegalArgumentException("INVALID_RULESET_HASH");
    }

    String declaredHash = declaredHashNode.asText();

    String expectedHash = hash(canonical(unsigned));

    if (!declaredHash.equals(expectedHash)) {
      throw new IllegalArgumentException("INVALID_RULESET_HASH");
    }
  }

  private String hash(JsonNode document) {
    try {
      byte[] canonicalPayload =
          MAPPER.writeValueAsString(document).getBytes(StandardCharsets.UTF_8);

      byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonicalPayload);

      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("RULESET_SERIALIZATION_FAILED", exception);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA_256_NOT_AVAILABLE", exception);
    }
  }

  private JsonNode canonical(JsonNode value) {
    if (value.isObject()) {
      ObjectNode ordered = MAPPER.createObjectNode();

      List<String> names = new ArrayList<>();

      value.fieldNames().forEachRemaining(names::add);

      names.stream()
          .sorted(Comparator.naturalOrder())
          .forEach(name -> ordered.set(name, canonical(value.get(name))));

      return ordered;
    }

    if (value.isArray()) {
      ArrayNode ordered = MAPPER.createArrayNode();

      value.forEach(item -> ordered.add(canonical(item)));

      return ordered;
    }

    return value;
  }
}
