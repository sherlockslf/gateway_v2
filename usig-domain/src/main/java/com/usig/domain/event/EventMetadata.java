package com.usig.domain.event;

import java.util.Map;
import java.util.Objects;

public record EventMetadata(String tenantId, String traceId, Map<String, String> attributes) {

  public EventMetadata {
    tenantId = requireText(tenantId, "tenantId");
    traceId = normalize(traceId);
    attributes = Map.copyOf(Objects.requireNonNullElse(attributes, Map.of()));
  }

  private static String requireText(String value, String name) {
    String normalized = normalize(value);
    if (normalized == null) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return normalized;
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
