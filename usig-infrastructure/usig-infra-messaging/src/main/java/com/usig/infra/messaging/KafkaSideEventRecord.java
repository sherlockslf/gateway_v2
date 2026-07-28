package com.usig.infra.messaging;

import com.usig.application.sideevent.SideEvent;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record KafkaSideEventRecord(
    String topic,
    String key,
    String eventId,
    String runId,
    String tenantId,
    String traceId,
    Instant occurredAt,
    Map<String, Object> payload) {

  public KafkaSideEventRecord {
    topic = requireText(topic, "topic");
    key = requireText(key, "key");
    eventId = requireText(eventId, "eventId");
    runId = requireText(runId, "runId");
    tenantId = requireText(tenantId, "tenantId");
    traceId = requireText(traceId, "traceId");
    occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    payload = Map.copyOf(payload == null ? Map.of() : payload);
  }

  static KafkaSideEventRecord fromSideEvent(SideEvent event) {
    return new KafkaSideEventRecord(
        event.topic(),
        event.runId(),
        event.eventId(),
        event.runId(),
        event.tenantId(),
        event.traceId(),
        event.occurredAt(),
        event.payload());
  }

  SideEvent toSideEvent() {
    return new SideEvent(topic, eventId, runId, tenantId, traceId, occurredAt, payload);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
