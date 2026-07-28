package com.usig.adapter.generic.sse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class GenericSseEventMapper {

  private final ObjectMapper objectMapper;
  private final GenericSseMappingProfile mappingProfile;

  public GenericSseEventMapper(ObjectMapper objectMapper, GenericSseMappingProfile mappingProfile) {
    this.objectMapper = objectMapper;
    this.mappingProfile = mappingProfile;
  }

  public GatewayEvent map(
      SseFrame frame,
      CreateRunCommand request,
      ExecutionContext context,
      String serviceId,
      String downstreamTaskId,
      long sequence,
      Instant observedAt) {
    JsonNode root = parse(frame.data());
    String sourceEventType = text(root.at(mappingProfile.sourceEventPointer()));
    EventType eventType = mappingProfile.mapEventType(sourceEventType);
    Map<String, String> variables = variables(request, context, sequence);
    Map<String, Object> payload = payload(root, variables);
    String sourceEventId = sourceEventId(frame, root, variables);
    return new GatewayEvent(
        GatewayEvent.CURRENT_SCHEMA_VERSION,
        "evt_" + context.runId().value() + "_" + String.format("%03d", sequence + 1),
        eventType,
        request.conversationId().value(),
        context.runId().value(),
        downstreamTaskId,
        serviceId,
        sourceEventId,
        sequence,
        observedAt,
        observedAt,
        payload,
        new EventMetadata(request.tenantId().value(), context.traceId(), context.attributes()));
  }

  private JsonNode parse(String data) {
    try {
      return objectMapper.readTree(data);
    } catch (JsonProcessingException ex) {
      throw new GenericSseAdapterException("SSE data is not valid JSON", ex);
    }
  }

  private Map<String, Object> payload(JsonNode root, Map<String, String> variables) {
    Map<String, Object> payload = new HashMap<>();
    for (Map.Entry<String, FieldMapping> entry : mappingProfile.payloadMappings().entrySet()) {
      Object value = entry.getValue().resolve(root, variables);
      if (value != null) {
        payload.put(entry.getKey(), value);
      }
    }
    return payload;
  }

  private String sourceEventId(SseFrame frame, JsonNode root, Map<String, String> variables) {
    if (mappingProfile.sourceEventIdMapping() != null) {
      Object value = mappingProfile.sourceEventIdMapping().resolve(root, variables);
      return value == null ? frame.id() : value.toString();
    }
    return frame.id();
  }

  private static Map<String, String> variables(
      CreateRunCommand request, ExecutionContext context, long sequence) {
    return Map.of(
        "conversationId",
        request.conversationId().value(),
        "runId",
        context.runId().value(),
        "tenantId",
        request.tenantId().value(),
        "sequence",
        Long.toString(sequence));
  }

  private static String text(JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      throw new GenericSseAdapterException("Mapped event type is missing");
    }
    if (!node.isTextual()) {
      throw new GenericSseAdapterException("Mapped event type must be a string");
    }
    return node.textValue();
  }
}
