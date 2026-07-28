package com.usig.application.sideevent;

import com.usig.application.port.SideEventOutbox;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public final class InMemorySideEventOutbox implements SideEventOutbox {

  private final int maxEntries;
  private final ArrayDeque<SideEventOutboxEntry> entries = new ArrayDeque<>();

  public InMemorySideEventOutbox(int maxEntries) {
    if (maxEntries < 1) {
      throw new IllegalArgumentException("maxEntries must be positive");
    }
    this.maxEntries = maxEntries;
  }

  @Override
  public CompletionStage<Void> recordFailure(SideEvent event, Throwable failure, Instant failedAt) {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(failure, "failure must not be null");
    Objects.requireNonNull(failedAt, "failedAt must not be null");
    synchronized (entries) {
      entries.addLast(SideEventOutboxEntry.fromFailure(event, failure, failedAt));
      while (entries.size() > maxEntries) {
        entries.removeFirst();
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<List<SideEventOutboxEntry>> pending(int maxEntries, Instant now) {
    if (maxEntries < 1) {
      throw new IllegalArgumentException("maxEntries must be positive");
    }
    Objects.requireNonNull(now, "now must not be null");
    synchronized (entries) {
      return CompletableFuture.completedFuture(
          entries.stream()
              .filter(entry -> !entry.nextAttemptAt().isAfter(now))
              .limit(maxEntries)
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Override
  public CompletionStage<Void> markPublished(String eventId) {
    String requiredEventId = requireText(eventId, "eventId");
    synchronized (entries) {
      entries.removeIf(entry -> requiredEventId.equals(entry.eventId()));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> markRetryFailure(
      String eventId, Throwable failure, Instant failedAt, Instant nextAttemptAt) {
    String requiredEventId = requireText(eventId, "eventId");
    Objects.requireNonNull(failure, "failure must not be null");
    Objects.requireNonNull(failedAt, "failedAt must not be null");
    Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
    synchronized (entries) {
      ArrayDeque<SideEventOutboxEntry> updated = new ArrayDeque<>();
      for (SideEventOutboxEntry entry : entries) {
        if (requiredEventId.equals(entry.eventId())) {
          updated.addLast(entry.withRetryFailure(failure, failedAt, nextAttemptAt));
        } else {
          updated.addLast(entry);
        }
      }
      entries.clear();
      entries.addAll(updated);
    }
    return CompletableFuture.completedFuture(null);
  }

  public List<SideEventOutboxEntry> snapshot() {
    synchronized (entries) {
      return List.copyOf(new ArrayList<>(entries));
    }
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
