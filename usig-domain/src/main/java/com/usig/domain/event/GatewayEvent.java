package com.usig.domain.event;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record GatewayEvent(
    String schemaVersion,
    String eventId,
    EventType eventType,
    String conversationId,
    String runId,
    String taskId,
    String sourceService,
    String sourceEventId,
    long sequence,
    Instant occurredAt,
    Instant observedAt,
    Map<String, Object> payload,
    EventMetadata metadata) {

  public static final String CURRENT_SCHEMA_VERSION = "1.0";

  public GatewayEvent {
    schemaVersion = requireText(schemaVersion, "schemaVersion");
    eventId = requireText(eventId, "eventId");
    eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    conversationId = requireText(conversationId, "conversationId");
    runId = requireText(runId, "runId");
    taskId = normalize(taskId);
    sourceService = requireText(sourceService, "sourceService");
    sourceEventId = normalize(sourceEventId);
    if (sequence < 0) {
      throw new IllegalArgumentException("sequence must not be negative");
    }
    observedAt = Objects.requireNonNull(observedAt, "observedAt must not be null");
    occurredAt = Objects.requireNonNullElse(occurredAt, observedAt);
    payload = Map.copyOf(Objects.requireNonNullElse(payload, Map.of()));
    metadata = Objects.requireNonNull(metadata, "metadata must not be null");
  }

  public static GatewayEvent of(
      String eventId,
      EventType eventType,
      String conversationId,
      String runId,
      String sourceService,
      long sequence,
      Instant observedAt,
      Map<String, Object> payload,
      EventMetadata metadata) {
    return new GatewayEvent(
        CURRENT_SCHEMA_VERSION,
        eventId,
        eventType,
        conversationId,
        runId,
        null,
        sourceService,
        null,
        sequence,
        observedAt,
        observedAt,
        payload,
        metadata);
  }

  public String wireEventType() {
    return eventType.wireName();
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
