package com.usig.infra.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class PostgresServiceRegistryTest {

  @Test
  void findsServiceDescriptorFromPostgresRecord() {
    FakePostgresServiceRegistryCommands commands =
        new FakePostgresServiceRegistryCommands(List.of(record(service("svc-db"))));
    PostgresServiceRegistry registry =
        new PostgresServiceRegistry(commands, Map.of("adapter-db", new FakeAdapter()));

    Optional<ServiceDescriptor> found = registry.find("svc-db");

    assertEquals(service("svc-db"), found.orElseThrow());
    assertEquals("svc-db", commands.lastFindServiceId);
  }

  @Test
  void listsServiceDescriptorsFromPostgresRecords() {
    ServiceDescriptor first = service("svc-a");
    ServiceDescriptor second = service("svc-b");
    FakePostgresServiceRegistryCommands commands =
        new FakePostgresServiceRegistryCommands(List.of(record(first), record(second)));
    PostgresServiceRegistry registry =
        new PostgresServiceRegistry(commands, Map.of("adapter-db", new FakeAdapter()));

    assertEquals(List.of(first, second), registry.list());
  }

  @Test
  void resolvesAdapterByDescriptorAdapterId() {
    FakeAdapter adapter = new FakeAdapter();
    PostgresServiceRegistry registry =
        new PostgresServiceRegistry(
            new FakePostgresServiceRegistryCommands(List.of()), Map.of("adapter-db", adapter));

    assertSame(adapter, registry.adapterFor(service("svc-db")));
  }

  @Test
  void rejectsMissingAdapterBindingAtRuntimeBoundary() {
    PostgresServiceRegistry registry =
        new PostgresServiceRegistry(new FakePostgresServiceRegistryCommands(List.of()), Map.of());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> registry.adapterFor(service("svc-db")));

    assertEquals("adapter not found: adapter-db", exception.getMessage());
  }

  @Test
  void propagatesCommandFailure() {
    FakePostgresServiceRegistryCommands commands =
        new FakePostgresServiceRegistryCommands(List.of());
    commands.failFind = true;
    PostgresServiceRegistry registry =
        new PostgresServiceRegistry(commands, Map.of("adapter-db", new FakeAdapter()));

    CompletionException exception =
        assertThrows(CompletionException.class, () -> registry.find("svc-db"));

    assertInstanceOf(IllegalStateException.class, exception.getCause());
  }

  private static ServiceDescriptor service(String serviceId) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://service.example/" + serviceId),
        "adapter-db",
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

  private static final class FakePostgresServiceRegistryCommands
      implements PostgresServiceRegistryCommands {

    private final List<PostgresServiceDescriptorRecord> records;
    private boolean failFind;
    private String lastFindServiceId;

    private FakePostgresServiceRegistryCommands(List<PostgresServiceDescriptorRecord> records) {
      this.records = List.copyOf(records);
    }

    @Override
    public CompletionStage<Optional<PostgresServiceDescriptorRecord>> findByServiceId(
        String serviceId) {
      lastFindServiceId = serviceId;
      if (failFind) {
        return CompletableFuture.failedFuture(new IllegalStateException("database unavailable"));
      }
      return CompletableFuture.completedFuture(
          records.stream().filter(record -> record.serviceId().equals(serviceId)).findFirst());
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
