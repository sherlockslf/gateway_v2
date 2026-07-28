package com.usig.adapter.business.workflow;

import java.util.Map;

public record BusinessWorkflowEvent(
    String type,
    String eventId,
    String nodeId,
    String messageId,
    String content,
    Integer progressPercent,
    String toolCallId,
    String toolName,
    String arguments,
    Object result,
    String artifactId,
    String artifactType,
    Object artifact,
    String inputRequestId,
    String prompt,
    Object schema,
    String approvalRequestId,
    String approvalName,
    Object approvalArguments,
    Map<String, Object> usage,
    Map<String, Object> raw) {

  public BusinessWorkflowEvent {
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("type must not be blank");
    }
    usage = Map.copyOf(usage == null ? Map.of() : usage);
    raw = Map.copyOf(raw == null ? Map.of() : raw);
  }

  public static BusinessWorkflowEvent of(String type, String eventId) {
    return new BusinessWorkflowEvent(
        type, eventId, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, Map.of(), Map.of());
  }
}
