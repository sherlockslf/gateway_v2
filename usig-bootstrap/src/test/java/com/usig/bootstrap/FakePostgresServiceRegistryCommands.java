package com.usig.bootstrap;

import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import com.usig.infra.postgres.PostgresServiceDescriptorRecord;
import com.usig.infra.postgres.PostgresServiceRegistryCommands;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public final class FakePostgresServiceRegistryCommands implements PostgresServiceRegistryCommands {

  private final List<PostgresServiceDescriptorRecord> records =
      List.of(PostgresServiceDescriptorRecord.fromDomain(postgresAgent()));

  @Override
  public CompletionStage<Optional<PostgresServiceDescriptorRecord>> findByServiceId(
      String serviceId) {
    return CompletableFuture.completedFuture(
        records.stream().filter(record -> record.serviceId().equals(serviceId)).findFirst());
  }

  @Override
  public CompletionStage<List<PostgresServiceDescriptorRecord>> list() {
    return CompletableFuture.completedFuture(records);
  }

  private static ServiceDescriptor postgresAgent() {
    return new ServiceDescriptor(
        "postgres-agent",
        ServiceType.AGENT,
        URI.create("https://internal.example/postgres-agent"),
        "sample-agent-v1",
        Set.of("tenant_pg"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(
            true, true, false, true, true, false, false, true, RetrySafety.NONE),
        "postgres-group",
        new ServiceLimits(11, 5, 9),
        6);
  }
}
