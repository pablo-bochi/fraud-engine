package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerde<T> implements Serde<T> {
  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  private final Class<T> type;

  public JsonSerde(Class<T> type) {
    this.type = type;
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, value) -> {
      try {
        return value == null ? null : MAPPER.writeValueAsBytes(value);
      } catch (IOException exception) {
        throw new IllegalArgumentException(exception);
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> {
      try {
        return bytes == null ? null : MAPPER.readValue(bytes, type);
      } catch (IOException exception) {
        throw new IllegalArgumentException(exception);
      }
    };
  }
}
