package com.usig.infra.postgres;

import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ServiceRegistry;
import com.usig.domain.service.ServiceDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class PostgresServiceRegistry implements ServiceRegistry {

  private final PostgresServiceRegistryCommands commands;
  private final Map<String, DownstreamExecutionPort> adaptersById;

  public PostgresServiceRegistry(
      PostgresServiceRegistryCommands commands, Map<String, DownstreamExecutionPort> adaptersById) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.adaptersById =
        Map.copyOf(Objects.requireNonNull(adaptersById, "adaptersById must not be null"));
  }

  @Override
  public Optional<ServiceDescriptor> find(String serviceId) {
    return commands
        .findByServiceId(serviceId)
        .toCompletableFuture()
        .join()
        .map(PostgresServiceDescriptorRecord::toDomain);
  }

  @Override
  public List<ServiceDescriptor> list() {
    return commands.list().toCompletableFuture().join().stream()
        .map(PostgresServiceDescriptorRecord::toDomain)
        .toList();
  }

  @Override
  public DownstreamExecutionPort adapterFor(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    DownstreamExecutionPort adapter = adaptersById.get(service.adapterId());
    if (adapter == null) {
      throw new IllegalStateException("adapter not found: " + service.adapterId());
    }
    return adapter;
  }
}
