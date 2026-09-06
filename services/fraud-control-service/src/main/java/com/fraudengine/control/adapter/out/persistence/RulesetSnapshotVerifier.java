package com.fraudengine.control.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.schema.SchemaValidator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;

final class RulesetSnapshotVerifier {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private RulesetSnapshotVerifier() {}

  static void verify(String canonicalPayload, long aggregateVersion) {
    try {
      JsonNode snapshot = OBJECT_MAPPER.readTree(canonicalPayload);
      if (!SchemaValidator.draft7().validate("ruleset-snapshot-v1", snapshot).isEmpty()) {
        throw new IllegalStateException("OUTBOX_SNAPSHOT_SCHEMA_INVALID");
      }
      if (snapshot.path("version").asLong(-1) != aggregateVersion) {
        throw new IllegalStateException("OUTBOX_SNAPSHOT_VERSION_INVALID");
      }
      if (!canonicalPayload.equals(json(canonical(snapshot)))) {
        throw new IllegalStateException("OUTBOX_SNAPSHOT_NOT_CANONICAL");
      }
      ObjectNode unsigned = ((ObjectNode) snapshot).deepCopy();
      String declaredHash = unsigned.remove("contentHash").asText();
      String expectedHash = hash(canonical(unsigned));
      if (!declaredHash.equals(expectedHash)) {
        throw new IllegalStateException("OUTBOX_SNAPSHOT_HASH_INVALID");
      }
    } catch (JsonProcessingException error) {
      throw new IllegalStateException("OUTBOX_SNAPSHOT_INVALID_JSON", error);
    }
  }

  private static String hash(JsonNode snapshot) {
    try {
      return "sha256:"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(json(snapshot).getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }

  private static String json(JsonNode value) {
    try {
      return OBJECT_MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException error) {
      throw new IllegalStateException("OUTBOX_SNAPSHOT_SERIALIZATION_FAILED", error);
    }
  }

  private static JsonNode canonical(JsonNode value) {
    if (value.isObject()) {
      ObjectNode ordered = OBJECT_MAPPER.createObjectNode();
      ArrayList<String> names = new ArrayList<>();
      value.fieldNames().forEachRemaining(names::add);
      names.stream()
          .sorted(Comparator.naturalOrder())
          .forEach(name -> ordered.set(name, canonical(value.get(name))));
      return ordered;
    }
    if (value.isArray()) {
      ArrayNode ordered = OBJECT_MAPPER.createArrayNode();
      value.forEach(item -> ordered.add(canonical(item)));
      return ordered;
    }
    return value;
  }
}
