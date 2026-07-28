package com.usig.application.sideevent;

import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record SideEvent(
    String topic,
    String eventId,
    String runId,
    String tenantId,
    String traceId,
    Instant occurredAt,
    Map<String, Object> payload) {

  public SideEvent {
    topic = requireText(topic, "topic");
    eventId = requireText(eventId, "eventId");
    runId = requireText(runId, "runId");
    tenantId = requireText(tenantId, "tenantId");
    traceId = requireText(traceId, "traceId");
    occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    payload = Map.copyOf(payload == null ? Map.of() : payload);
  }

  public static SideEvent fromGatewayEvent(
      String topic, GatewayEvent gatewayEvent, Map<String, Object> payload) {
    Objects.requireNonNull(gatewayEvent, "gatewayEvent must not be null");
    return new SideEvent(
        topic,
        gatewayEvent.eventId(),
        gatewayEvent.runId(),
        gatewayEvent.metadata().tenantId(),
        gatewayEvent.metadata().traceId(),
        gatewayEvent.observedAt(),
        payload);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
