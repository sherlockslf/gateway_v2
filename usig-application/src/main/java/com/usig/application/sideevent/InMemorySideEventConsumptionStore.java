package com.usig.application.sideevent;

import com.usig.application.port.SideEventConsumptionStore;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class InMemorySideEventConsumptionStore implements SideEventConsumptionStore {

  private final int maxConsumedEventIds;
  private final Set<String> consumed = new HashSet<>();
  private final Set<String> inProgress = new HashSet<>();
  private final Deque<String> consumedOrder = new ArrayDeque<>();

  public InMemorySideEventConsumptionStore(int maxConsumedEventIds) {
    if (maxConsumedEventIds <= 0) {
      throw new IllegalArgumentException("maxConsumedEventIds must be positive");
    }
    this.maxConsumedEventIds = maxConsumedEventIds;
  }

  @Override
  public synchronized CompletionStage<SideEventConsumptionClaim> claim(String eventId) {
    String key = requireText(eventId, "eventId");
    if (consumed.contains(key)) {
      return CompletableFuture.completedFuture(SideEventConsumptionClaim.CONSUMED);
    }
    if (inProgress.contains(key)) {
      return CompletableFuture.completedFuture(SideEventConsumptionClaim.IN_PROGRESS);
    }
    inProgress.add(key);
    return CompletableFuture.completedFuture(SideEventConsumptionClaim.FIRST);
  }

  @Override
  public synchronized CompletionStage<Void> markConsumed(String eventId) {
    String key = requireText(eventId, "eventId");
    inProgress.remove(key);
    if (consumed.add(key)) {
      consumedOrder.addLast(key);
      while (consumedOrder.size() > maxConsumedEventIds) {
        consumed.remove(consumedOrder.removeFirst());
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletionStage<Void> release(String eventId) {
    inProgress.remove(requireText(eventId, "eventId"));
    return CompletableFuture.completedFuture(null);
  }

  public synchronized Set<String> consumedSnapshot() {
    return Set.copyOf(consumed);
  }

  public synchronized Set<String> inProgressSnapshot() {
    return Set.copyOf(inProgress);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
