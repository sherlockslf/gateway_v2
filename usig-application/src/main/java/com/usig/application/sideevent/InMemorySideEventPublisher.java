package com.usig.application.sideevent;

import com.usig.application.port.SideEventPublisher;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class InMemorySideEventPublisher implements SideEventPublisher {

  private final int maxEvents;
  private final ArrayDeque<SideEvent> events = new ArrayDeque<>();

  public InMemorySideEventPublisher(int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    this.maxEvents = maxEvents;
  }

  @Override
  public CompletionStage<Void> publish(SideEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    synchronized (events) {
      events.addLast(event);
      while (events.size() > maxEvents) {
        events.removeFirst();
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  public List<SideEvent> snapshot() {
    synchronized (events) {
      return List.copyOf(new ArrayList<>(events));
    }
  }
}
