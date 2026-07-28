package com.usig.application.memory;

import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.port.ServiceRegistryAuditPublisher;
import com.usig.application.port.ServiceRegistryReloader;
import com.usig.application.serviceaudit.ServiceRegistryAuditEvent;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryServiceRegistry implements ServiceRegistry, ServiceRegistryReloader {

  private final Map<String, ServiceDescriptor> services = new ConcurrentHashMap<>();
  private final Map<String, DownstreamExecutionPort> adapters = new ConcurrentHashMap<>();
  private final ServiceRegistryAuditPublisher auditPublisher;
  private final java.util.function.Supplier<Instant> clock;

  public InMemoryServiceRegistry() {
    this(event -> java.util.concurrent.CompletableFuture.completedFuture(null), Instant::now);
  }

  public InMemoryServiceRegistry(
      ServiceRegistryAuditPublisher auditPublisher, java.util.function.Supplier<Instant> clock) {
    this.auditPublisher = Objects.requireNonNull(auditPublisher, "auditPublisher must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public void register(ServiceDescriptor service, DownstreamExecutionPort adapter) {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(adapter, "adapter must not be null");
    ServiceDescriptor previous = services.get(service.serviceId());
    String action = previous == null ? "service.registered" : "service.updated";
    auditPublisher
        .publish(ServiceRegistryAuditEvent.fromDescriptor(action, service, clock.get()))
        .toCompletableFuture()
        .join();
    services.put(service.serviceId(), service);
    adapters.put(service.adapterId(), adapter);
  }

  @Override
  public void reload(Collection<ServiceRegistryEntry> entries) {
    Objects.requireNonNull(entries, "entries must not be null");
    Map<String, ServiceDescriptor> nextServices = new LinkedHashMap<>();
    Map<String, DownstreamExecutionPort> nextAdapters = new LinkedHashMap<>();
    for (ServiceRegistryEntry entry : entries) {
      ServiceDescriptor previous = nextServices.put(entry.service().serviceId(), entry.service());
      if (previous != null) {
        throw new IllegalArgumentException("duplicate serviceId: " + entry.service().serviceId());
      }
      nextAdapters.put(entry.service().adapterId(), entry.adapter());
    }

    for (ServiceDescriptor service : nextServices.values()) {
      ServiceDescriptor previous = services.get(service.serviceId());
      String action = previous == null ? "service.registered" : "service.updated";
      auditPublisher
          .publish(ServiceRegistryAuditEvent.fromDescriptor(action, service, clock.get()))
          .toCompletableFuture()
          .join();
    }
    for (ServiceDescriptor removed : services.values()) {
      if (!nextServices.containsKey(removed.serviceId())) {
        auditPublisher
            .publish(ServiceRegistryAuditEvent.removed(removed, clock.get()))
            .toCompletableFuture()
            .join();
      }
    }

    services.clear();
    services.putAll(nextServices);
    adapters.clear();
    adapters.putAll(nextAdapters);
  }

  @Override
  public Optional<ServiceDescriptor> find(String serviceId) {
    return Optional.ofNullable(services.get(serviceId));
  }

  @Override
  public List<ServiceDescriptor> list() {
    return List.copyOf(services.values());
  }

  @Override
  public DownstreamExecutionPort adapterFor(ServiceDescriptor service) {
    DownstreamExecutionPort adapter = adapters.get(service.adapterId());
    if (adapter == null) {
      throw new IllegalStateException("adapter not found: " + service.adapterId());
    }
    return adapter;
  }
}
