package com.usig.domain.event;

import java.util.Locale;

public enum EventType {
  EXECUTION_ACCEPTED("execution.accepted"),
  EXECUTION_STARTED("execution.started"),
  EXECUTION_RUNNING("execution.running"),
  EXECUTION_COMPLETED("execution.completed"),
  EXECUTION_FAILED("execution.failed"),
  EXECUTION_CANCEL_REQUESTED("execution.cancel_requested"),
  EXECUTION_CANCELED("execution.canceled"),
  TEXT_START("text.start"),
  TEXT_DELTA("text.delta"),
  TEXT_END("text.end"),
  TOOL_START("tool.start"),
  TOOL_ARGUMENTS_DELTA("tool.arguments.delta"),
  TOOL_RESULT("tool.result"),
  TOOL_ERROR("tool.error"),
  TOOL_END("tool.end"),
  ARTIFACT_CREATED("artifact.created"),
  ARTIFACT_UPDATED("artifact.updated"),
  ARTIFACT_COMPLETED("artifact.completed"),
  INPUT_REQUIRED("input.required"),
  APPROVAL_REQUIRED("approval.required"),
  PROGRESS_UPDATED("progress.updated"),
  USAGE_UPDATED("usage.updated"),
  WARNING("warning"),
  HEARTBEAT("heartbeat"),
  EXTENSION("extension");

  private final String wireName;

  EventType(String wireName) {
    this.wireName = wireName;
  }

  public String wireName() {
    return wireName;
  }

  public static EventType fromWireName(String wireName) {
    if (wireName == null || wireName.isBlank()) {
      throw new IllegalArgumentException("eventType must not be blank");
    }
    String normalized = wireName.toLowerCase(Locale.ROOT);
    for (EventType eventType : values()) {
      if (eventType.wireName.equals(normalized)) {
        return eventType;
      }
    }
    throw new IllegalArgumentException("Unsupported eventType: " + wireName);
  }

  public boolean terminal() {
    return this == EXECUTION_COMPLETED || this == EXECUTION_FAILED || this == EXECUTION_CANCELED;
  }
}
