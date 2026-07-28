package com.usig.api.dto;

import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record GatewayEventDto(
    String schemaVersion,
    String eventId,
    String eventType,
    String conversationId,
    String runId,
    String taskId,
    String sourceService,
    String sourceEventId,
    long sequence,
    Instant occurredAt,
    Instant observedAt,
    Map<String, Object> payload,
    Map<String, Object> metadata) {

  public static GatewayEventDto from(GatewayEvent event) {
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("tenantId", event.metadata().tenantId());
    metadata.put("traceId", event.metadata().traceId());
    metadata.put("attributes", event.metadata().attributes());
    return new GatewayEventDto(
        event.schemaVersion(),
        event.eventId(),
        event.wireEventType(),
        event.conversationId(),
        event.runId(),
        event.taskId(),
        event.sourceService(),
        event.sourceEventId(),
        event.sequence(),
        event.occurredAt(),
        event.observedAt(),
        event.payload(),
        Collections.unmodifiableMap(metadata));
  }
}
