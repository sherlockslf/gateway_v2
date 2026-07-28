package com.usig.infra.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.memory.ServiceRegistryEntry;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class PostgresServiceRegistrySnapshotSourceTest {

  @Test
  void loadsReloadEntriesFromPostgresRecords() {
    ServiceDescriptor first = service("svc-a", "adapter-db");
    ServiceDescriptor second = service("svc-b", "adapter-db");
    FakeAdapter adapter = new FakeAdapter();
    PostgresServiceRegistrySnapshotSource source =
        new PostgresServiceRegistrySnapshotSource(
            new FakeCommands(List.of(record(first), record(second))),
            Map.of("adapter-db", adapter));

    List<ServiceRegistryEntry> entries = source.load().toCompletableFuture().join();

    assertEquals(
        List.of(first, second), entries.stream().map(ServiceRegistryEntry::service).toList());
    assertSame(adapter, entries.get(0).adapter());
    assertSame(adapter, entries.get(1).adapter());
  }

  @Test
  void rejectsUnknownAdapterBeforeReloadBoundary() {
    PostgresServiceRegistrySnapshotSource source =
        new PostgresServiceRegistrySnapshotSource(
            new FakeCommands(List.of(record(service("svc-a", "missing-adapter")))), Map.of());

    CompletionException exception =
        assertThrows(CompletionException.class, () -> source.load().toCompletableFuture().join());

    assertEquals("adapter not found: missing-adapter", exception.getCause().getMessage());
  }

  private static ServiceDescriptor service(String serviceId, String adapterId) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://service.example/" + serviceId),
        adapterId,
        Set.of("tenant-a"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(
            true, true, false, true, true, false, false, true, RetrySafety.NONE),
        "agent-group",
        new ServiceLimits(100, 20, 80),
        3);
  }

  private static PostgresServiceDescriptorRecord record(ServiceDescriptor service) {
    return PostgresServiceDescriptorRecord.fromDomain(service);
  }

  private record FakeCommands(List<PostgresServiceDescriptorRecord> records)
      implements PostgresServiceRegistryCommands {

    @Override
    public CompletionStage<java.util.Optional<PostgresServiceDescriptorRecord>> findByServiceId(
        String serviceId) {
      return CompletableFuture.completedFuture(java.util.Optional.empty());
    }

    @Override
    public CompletionStage<List<PostgresServiceDescriptorRecord>> list() {
      return CompletableFuture.completedFuture(records);
    }
  }

  private static final class FakeAdapter implements DownstreamExecutionPort {

    @Override
    public CompletionStage<AdapterExecution> execute(
        CreateRunCommand command, ExecutionContext context) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletionStage<DownstreamExecutionState> cancel(
        ExecutionHandle handle, ExecutionContext context) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletionStage<DownstreamExecutionState> queryState(
        ExecutionHandle handle, ExecutionContext context) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
        ExecutionHandle handle, String cursor, ExecutionContext context) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
  }
}
