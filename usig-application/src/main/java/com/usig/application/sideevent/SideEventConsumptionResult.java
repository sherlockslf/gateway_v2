package com.usig.application.sideevent;

import java.util.Objects;

public record SideEventConsumptionResult(String eventId, Status status) {

  public SideEventConsumptionResult {
    eventId = requireText(eventId, "eventId");
    status = Objects.requireNonNull(status, "status must not be null");
  }

  public static SideEventConsumptionResult processed(String eventId) {
    return new SideEventConsumptionResult(eventId, Status.PROCESSED);
  }

  public static SideEventConsumptionResult skippedInProgress(String eventId) {
    return new SideEventConsumptionResult(eventId, Status.SKIPPED_IN_PROGRESS);
  }

  public static SideEventConsumptionResult skippedConsumed(String eventId) {
    return new SideEventConsumptionResult(eventId, Status.SKIPPED_CONSUMED);
  }

  public enum Status {
    PROCESSED,
    SKIPPED_IN_PROGRESS,
    SKIPPED_CONSUMED
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
