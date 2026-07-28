package com.usig.adapter.agent.a2a;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class AgentA2aEventMapper {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final ObjectMapper objectMapper;

  AgentA2aEventMapper(ObjectMapper objectMapper) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
  }

  GatewayEvent map(
      AgentA2aSseFrame frame,
      CreateRunCommand request,
      ExecutionContext context,
      String serviceId,
      String downstreamTaskId,
      long sequence,
      Instant now) {
    JsonNode root = parse(frame.data());
    String downstreamType = text(root, "type");
    EventType eventType = eventType(root, downstreamType);
    Map<String, Object> payload = payload(root, downstreamType, downstreamTaskId);
    return new GatewayEvent(
        GatewayEvent.CURRENT_SCHEMA_VERSION,
        "evt_" + context.runId().value() + "_" + String.format("%03d", sequence + 1),
        eventType,
        request.conversationId().value(),
        context.runId().value(),
        downstreamTaskId,
        serviceId,
        sourceEventId(frame, root, context, sequence),
        sequence,
        now,
        now,
        payload,
        new EventMetadata(request.tenantId().value(), context.traceId(), Map.of()));
  }

  private JsonNode parse(String data) {
    try {
      return objectMapper.readTree(data);
    } catch (Exception ex) {
      throw new AgentA2aAdapterException("Agent A2A SSE data is not valid JSON", ex);
    }
  }

  private EventType eventType(JsonNode root, String downstreamType) {
    return switch (downstreamType) {
      case "task.status" -> taskStatusEventType(text(root, "status"));
      case "message.part" ->
          "text".equals(text(root, "part_type"))
              ? EventType.TEXT_DELTA
              : EventType.ARTIFACT_UPDATED;
      case "artifact.created" -> EventType.ARTIFACT_CREATED;
      case "artifact.updated" -> EventType.ARTIFACT_UPDATED;
      case "artifact.completed" -> EventType.ARTIFACT_COMPLETED;
      case "input.required" -> EventType.INPUT_REQUIRED;
      case "approval.required" -> EventType.APPROVAL_REQUIRED;
      default -> EventType.EXTENSION;
    };
  }

  private EventType taskStatusEventType(String status) {
    return switch (status) {
      case "submitted", "started" -> EventType.EXECUTION_STARTED;
      case "working", "running" -> EventType.EXECUTION_RUNNING;
      case "waiting_input" -> EventType.INPUT_REQUIRED;
      case "completed" -> EventType.EXECUTION_COMPLETED;
      case "failed" -> EventType.EXECUTION_FAILED;
      case "canceled" -> EventType.EXECUTION_CANCELED;
      default -> EventType.PROGRESS_UPDATED;
    };
  }

  private Map<String, Object> payload(
      JsonNode root, String downstreamType, String downstreamTaskId) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("downstreamEventType", downstreamType);
    payload.put("taskId", valueOrFallback(text(root, "task_id"), downstreamTaskId));
    switch (downstreamType) {
      case "task.status" -> addTaskStatusPayload(payload, root);
      case "message.part" -> addMessagePartPayload(payload, root);
      case "artifact.created", "artifact.updated", "artifact.completed" ->
          addArtifactPayload(payload, root);
      case "input.required" -> addInputRequiredPayload(payload, root);
      case "approval.required" -> addApprovalRequiredPayload(payload, root);
      default -> addRawPayload(payload, root);
    }
    return Map.copyOf(payload);
  }

  private void addTaskStatusPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("status", text(root, "status"));
    putIfText(payload, "message", text(root, "message"));
    copyObject(payload, "details", root.get("details"));
  }

  private void addMessagePartPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("messageId", text(root, "message_id"));
    payload.put("partType", text(root, "part_type"));
    if ("text".equals(text(root, "part_type"))) {
      payload.put("content", text(root, "text"));
    } else {
      payload.put("artifactType", "agent.message.part");
      copyObject(payload, "part", root.get("part"));
    }
  }

  private void addArtifactPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("artifactId", text(root, "artifact_id"));
    payload.put("artifactType", valueOrFallback(text(root, "artifact_type"), "agent.artifact"));
    putIfText(payload, "name", text(root, "name"));
    copyObject(payload, "artifact", root.get("artifact"));
  }

  private void addInputRequiredPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("inputRequestId", text(root, "input_request_id"));
    putIfText(payload, "prompt", text(root, "prompt"));
    copyObject(payload, "schema", root.get("schema"));
  }

  private void addApprovalRequiredPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("approvalRequestId", text(root, "approval_request_id"));
    putIfText(payload, "name", text(root, "name"));
    copyObject(payload, "arguments", root.get("arguments"));
  }

  private void addRawPayload(Map<String, Object> payload, JsonNode root) {
    payload.put("extensionType", "agent.a2a.raw");
    payload.put("rawEvent", objectMapper.convertValue(root, MAP_TYPE));
  }

  private void copyObject(Map<String, Object> payload, String name, JsonNode node) {
    if (node != null && !node.isNull()) {
      payload.put(name, objectMapper.convertValue(node, MAP_TYPE));
    }
  }

  private void putIfText(Map<String, Object> payload, String name, String value) {
    if (value != null && !value.isBlank()) {
      payload.put(name, value);
    }
  }

  private String sourceEventId(
      AgentA2aSseFrame frame, JsonNode root, ExecutionContext context, long sequence) {
    String sourceId = frame.id();
    if (sourceId == null) {
      sourceId = text(root, "event_id");
    }
    if (sourceId == null) {
      sourceId = "agent_" + context.runId().value() + "_" + sequence;
    }
    return sourceId;
  }

  private String text(JsonNode root, String field) {
    JsonNode value = root.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  private String valueOrFallback(String value, String fallback) {
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return value;
  }
}
