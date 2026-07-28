package com.usig.infra.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

final class PostgresExecutionHandleJson {

  private final ObjectMapper objectMapper;

  PostgresExecutionHandleJson(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  String serialize(PostgresExecutionHandleRecord handle) {
    if (handle == null) {
      return null;
    }
    try {
      return objectMapper.writeValueAsString(handle);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("execution handle cannot be serialized", exception);
    }
  }

  PostgresExecutionHandleRecord deserialize(String json) {
    if (json == null || json.isBlank()) {
      return null;
    }
    try {
      return objectMapper.readValue(json, PostgresExecutionHandleRecord.class);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("execution handle cannot be deserialized", exception);
    }
  }
}
