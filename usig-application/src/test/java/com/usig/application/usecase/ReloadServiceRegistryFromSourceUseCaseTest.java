package com.usig.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class ReloadServiceRegistryFromSourceUseCaseTest {

  @Test
  void reloadsSnapshotLoadedFromSource() {
    ServiceRegistryEntry entry = new ServiceRegistryEntry(service("svc-source"), new FakeAdapter());
    List<List<ServiceRegistryEntry>> reloaded = new ArrayList<>();
    ReloadServiceRegistryFromSourceUseCase useCase =
        new ReloadServiceRegistryFromSourceUseCase(
            () -> CompletableFuture.completedFuture(List.of(entry)),
            entries -> reloaded.add(List.copyOf(entries)));

    ServiceRegistrySourceReloadResult result = useCase.reload().toCompletableFuture().join();

    assertEquals(1, result.serviceCount());
    assertEquals(List.of(List.of(entry)), reloaded);
  }

  @Test
  void propagatesSourceFailureWithoutReloadingTarget() {
    List<List<ServiceRegistryEntry>> reloaded = new ArrayList<>();
    ReloadServiceRegistryFromSourceUseCase useCase =
        new ReloadServiceRegistryFromSourceUseCase(
            () -> CompletableFuture.failedFuture(new IllegalStateException("source unavailable")),
            entries -> reloaded.add(List.copyOf(entries)));

    CompletionException exception =
        assertThrows(
            CompletionException.class, () -> useCase.reload().toCompletableFuture().join());

    assertEquals("source unavailable", exception.getCause().getMessage());
    assertEquals(List.of(), reloaded);
  }

  private static ServiceDescriptor service(String serviceId) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://service.example/" + serviceId),
        "adapter-source",
        Set.of("tenant-a"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(
            true, true, false, true, true, false, false, true, RetrySafety.NONE),
        "source-group",
        new ServiceLimits(100, 20, 80),
        3);
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
