package com.usig.domain.run;

import java.util.Map;
import java.util.Objects;

public record ExecutionHandle(
    String serviceId,
    String adapterId,
    String downstreamTaskId,
    String downstreamThreadId,
    String downstreamRunId,
    Map<String, String> opaqueAttributes) {

  private static final int MAX_OPAQUE_ENTRIES = 32;
  private static final int MAX_OPAQUE_KEY_LENGTH = 64;
  private static final int MAX_OPAQUE_VALUE_LENGTH = 512;

  public ExecutionHandle {
    serviceId = requireText(serviceId, "serviceId");
    adapterId = requireText(adapterId, "adapterId");
    downstreamTaskId = normalize(downstreamTaskId);
    downstreamThreadId = normalize(downstreamThreadId);
    downstreamRunId = normalize(downstreamRunId);
    opaqueAttributes = Map.copyOf(Objects.requireNonNullElse(opaqueAttributes, Map.of()));
    validateOpaqueAttributes(opaqueAttributes);
  }

  private static void validateOpaqueAttributes(Map<String, String> attributes) {
    if (attributes.size() > MAX_OPAQUE_ENTRIES) {
      throw new IllegalArgumentException("opaqueAttributes exceeds maximum entry count");
    }
    attributes.forEach(
        (key, value) -> {
          if (key == null || key.isBlank() || key.length() > MAX_OPAQUE_KEY_LENGTH) {
            throw new IllegalArgumentException("opaqueAttributes contains an invalid key");
          }
          if (value == null || value.length() > MAX_OPAQUE_VALUE_LENGTH) {
            throw new IllegalArgumentException("opaqueAttributes contains an invalid value");
          }
        });
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
