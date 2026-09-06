package com.fraudengine.contracts.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class SchemaValidator {

  private final JsonSchemaFactory factory;
  private final Map<String, JsonSchema> schemas = new ConcurrentHashMap<>();

  private SchemaValidator(JsonSchemaFactory factory) {
    this.factory = factory;
  }

  public static SchemaValidator draft7() {
    return new SchemaValidator(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7));
  }

  public Set<ValidationMessage> validate(String contractName, JsonNode document) {
    return schemas.computeIfAbsent(contractName, this::load).validate(document);
  }

  private JsonSchema load(String contractName) {
    String resource = "/schemas/" + contractName + ".schema.json";
    InputStream stream = SchemaValidator.class.getResourceAsStream(resource);
    if (stream == null) {
      throw new IllegalArgumentException("Unknown contract: " + contractName);
    }
    return factory.getSchema(stream);
  }
}
