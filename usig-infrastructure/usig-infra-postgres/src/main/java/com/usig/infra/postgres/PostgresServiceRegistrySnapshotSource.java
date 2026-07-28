package com.usig.infra.postgres;

import com.usig.application.memory.ServiceRegistryEntry;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ServiceRegistrySnapshotSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class PostgresServiceRegistrySnapshotSource implements ServiceRegistrySnapshotSource {

  private final PostgresServiceRegistryCommands commands;
  private final Map<String, DownstreamExecutionPort> adaptersById;

  public PostgresServiceRegistrySnapshotSource(
      PostgresServiceRegistryCommands commands, Map<String, DownstreamExecutionPort> adaptersById) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.adaptersById =
        Map.copyOf(Objects.requireNonNull(adaptersById, "adaptersById must not be null"));
  }

  @Override
  public CompletionStage<List<ServiceRegistryEntry>> load() {
    return commands
        .list()
        .thenApply(
            records ->
                records.stream()
                    .map(PostgresServiceDescriptorRecord::toDomain)
                    .map(service -> new ServiceRegistryEntry(service, adapter(service.adapterId())))
                    .toList());
  }

  private DownstreamExecutionPort adapter(String adapterId) {
    DownstreamExecutionPort adapter = adaptersById.get(adapterId);
    if (adapter == null) {
      throw new IllegalStateException("adapter not found: " + adapterId);
    }
    return adapter;
  }
}
