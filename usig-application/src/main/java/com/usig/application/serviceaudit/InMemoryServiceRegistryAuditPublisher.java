package com.usig.application.serviceaudit;

import com.usig.application.port.ServiceRegistryAuditPublisher;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class InMemoryServiceRegistryAuditPublisher implements ServiceRegistryAuditPublisher {

  private final int maxEvents;
  private final ArrayDeque<ServiceRegistryAuditEvent> events = new ArrayDeque<>();

  public InMemoryServiceRegistryAuditPublisher(int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    this.maxEvents = maxEvents;
  }

  @Override
  public CompletionStage<Void> publish(ServiceRegistryAuditEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    synchronized (events) {
      events.addLast(event);
      while (events.size() > maxEvents) {
        events.removeFirst();
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  public List<ServiceRegistryAuditEvent> snapshot() {
    synchronized (events) {
      return List.copyOf(new ArrayList<>(events));
    }
  }
}
