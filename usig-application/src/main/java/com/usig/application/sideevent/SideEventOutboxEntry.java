package com.usig.application.sideevent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record SideEventOutboxEntry(
    String topic,
    String eventId,
    String runId,
    String tenantId,
    String traceId,
    Instant occurredAt,
    Instant failedAt,
    Instant nextAttemptAt,
    int attemptCount,
    String failureType,
    String failureMessage,
    Map<String, Object> payload) {

  public SideEventOutboxEntry {
    topic = requireText(topic, "topic");
    eventId = requireText(eventId, "eventId");
    runId = requireText(runId, "runId");
    tenantId = requireText(tenantId, "tenantId");
    traceId = requireText(traceId, "traceId");
    occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    failedAt = Objects.requireNonNull(failedAt, "failedAt must not be null");
    nextAttemptAt = Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be positive");
    }
    failureType = requireText(failureType, "failureType");
    failureMessage = failureMessage == null ? "" : failureMessage;
    payload = Map.copyOf(payload == null ? Map.of() : payload);
  }

  static SideEventOutboxEntry fromFailure(SideEvent event, Throwable failure, Instant failedAt) {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(failure, "failure must not be null");
    Throwable root = unwrap(failure);
    return new SideEventOutboxEntry(
        event.topic(),
        event.eventId(),
        event.runId(),
        event.tenantId(),
        event.traceId(),
        event.occurredAt(),
        failedAt,
        failedAt,
        1,
        root.getClass().getName(),
        root.getMessage(),
        event.payload());
  }

  SideEventOutboxEntry withRetryFailure(
      Throwable failure, Instant failedAt, Instant nextAttemptAt) {
    Objects.requireNonNull(failure, "failure must not be null");
    Throwable root = unwrap(failure);
    return new SideEventOutboxEntry(
        topic,
        eventId,
        runId,
        tenantId,
        traceId,
        occurredAt,
        failedAt,
        nextAttemptAt,
        attemptCount + 1,
        root.getClass().getName(),
        root.getMessage(),
        payload);
  }

  public SideEvent toSideEvent() {
    return new SideEvent(topic, eventId, runId, tenantId, traceId, occurredAt, payload);
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null
        && ("java.util.concurrent.CompletionException".equals(current.getClass().getName())
            || "java.util.concurrent.ExecutionException".equals(current.getClass().getName()))) {
      current = current.getCause();
    }
    return current;
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
