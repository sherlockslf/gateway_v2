package com.usig.adapter.business.workflow;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

final class BusinessWorkflowEventMapper {

  GatewayEvent map(
      BusinessWorkflowEvent event,
      CreateRunCommand request,
      ExecutionContext context,
      String serviceId,
      String workflowRunId,
      long sequence,
      Instant now) {
    return new GatewayEvent(
        GatewayEvent.CURRENT_SCHEMA_VERSION,
        "evt_" + context.runId().value() + "_" + String.format("%03d", sequence + 1),
        eventType(event.type()),
        request.conversationId().value(),
        context.runId().value(),
        workflowRunId,
        serviceId,
        sourceEventId(event, context, sequence),
        sequence,
        now,
        now,
        payload(event, workflowRunId),
        new EventMetadata(request.tenantId().value(), context.traceId(), Map.of()));
  }

  private EventType eventType(String type) {
    return switch (type) {
      case "workflow.started" -> EventType.EXECUTION_STARTED;
      case "workflow.running" -> EventType.EXECUTION_RUNNING;
      case "workflow.completed" -> EventType.EXECUTION_COMPLETED;
      case "workflow.failed" -> EventType.EXECUTION_FAILED;
      case "workflow.canceled" -> EventType.EXECUTION_CANCELED;
      case "node.started", "node.progress", "node.completed" -> EventType.PROGRESS_UPDATED;
      case "retrieval.document" -> EventType.ARTIFACT_UPDATED;
      case "answer.delta" -> EventType.TEXT_DELTA;
      case "answer.done" -> EventType.TEXT_END;
      case "tool.started" -> EventType.TOOL_START;
      case "tool.arguments" -> EventType.TOOL_ARGUMENTS_DELTA;
      case "tool.result" -> EventType.TOOL_RESULT;
      case "tool.failed" -> EventType.TOOL_ERROR;
      case "tool.completed" -> EventType.TOOL_END;
      case "artifact.created" -> EventType.ARTIFACT_CREATED;
      case "artifact.updated" -> EventType.ARTIFACT_UPDATED;
      case "artifact.completed" -> EventType.ARTIFACT_COMPLETED;
      case "input.required" -> EventType.INPUT_REQUIRED;
      case "approval.required" -> EventType.APPROVAL_REQUIRED;
      case "usage.updated" -> EventType.USAGE_UPDATED;
      default -> EventType.EXTENSION;
    };
  }

  private Map<String, Object> payload(BusinessWorkflowEvent event, String workflowRunId) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("downstreamEventType", event.type());
    payload.put("workflowRunId", workflowRunId);
    putIfText(payload, "nodeId", event.nodeId());
    putIfText(payload, "messageId", event.messageId());
    putIfText(payload, "content", event.content());
    putIfPresent(payload, "progressPercent", event.progressPercent());
    putIfText(payload, "toolCallId", event.toolCallId());
    putIfText(payload, "toolName", event.toolName());
    putIfText(payload, "arguments", event.arguments());
    putIfPresent(payload, "result", event.result());
    putIfText(payload, "artifactId", event.artifactId());
    putIfText(payload, "artifactType", event.artifactType());
    putIfPresent(payload, "artifact", event.artifact());
    putIfText(payload, "inputRequestId", event.inputRequestId());
    putIfText(payload, "prompt", event.prompt());
    putIfPresent(payload, "schema", event.schema());
    putIfText(payload, "approvalRequestId", event.approvalRequestId());
    putIfText(payload, "name", event.approvalName());
    putIfPresent(payload, "arguments", event.approvalArguments());
    payload.putAll(event.usage());
    if (eventType(event.type()) == EventType.EXTENSION) {
      payload.put("extensionType", "business.workflow.raw");
      payload.put("rawEvent", event.raw().isEmpty() ? Map.of("type", event.type()) : event.raw());
    }
    return Map.copyOf(payload);
  }

  private String sourceEventId(
      BusinessWorkflowEvent event, ExecutionContext context, long sequence) {
    if (event.eventId() != null && !event.eventId().isBlank()) {
      return event.eventId();
    }
    return "workflow_" + context.runId().value() + "_" + sequence;
  }

  private void putIfText(Map<String, Object> payload, String name, String value) {
    if (value != null && !value.isBlank()) {
      payload.put(name, value);
    }
  }

  private void putIfPresent(Map<String, Object> payload, String name, Object value) {
    if (value != null) {
      payload.put(name, value);
    }
  }
}
