package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.TransactionEvent;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

final class EventFingerprint {

  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  private EventFingerprint() {}

  static String of(TransactionEvent event) {
    try {
      byte[] serialized = MAPPER.writeValueAsBytes(event);

      byte[] digest = MessageDigest.getInstance("SHA-256").digest(serialized);

      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("EVENT_FINGERPRINT_SERIALIZATION_FAILED", exception);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA_256_NOT_AVAILABLE", exception);
    }
  }
}
