package com.usig.application.serviceaudit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.memory.InMemoryServiceRegistry;
import com.usig.application.memory.ServiceRegistryEntry;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.ServiceRegistryAuditPublisher;
import com.usig.application.sideevent.InMemorySideEventOutbox;
import com.usig.application.sideevent.InMemorySideEventPublisher;
import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventDispatchPolicy;
import com.usig.application.sideevent.SideEventDispatcher;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ServiceRegistryAuditTest {

  private static final Instant NOW = Instant.parse("2026-07-27T12:30:00Z");

  @Test
  void publishesAuditEventWhenServiceIsRegistered() {
    InMemoryServiceRegistryAuditPublisher auditPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);

    registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort());

    ServiceRegistryAuditEvent event = auditPublisher.snapshot().getFirst();
    assertEquals("service-registry-agent-1-v1", event.eventId());
    assertEquals("service.registered", event.action());
    assertEquals("agent-1", event.serviceId());
    assertEquals(1, event.version());
    assertEquals(NOW, event.occurredAt());
    assertEquals("sample-adapter", event.payload().get("adapterId"));
    assertEquals("AGENT", event.payload().get("serviceType"));
    assertEquals("https://internal.example/agent-1", event.payload().get("endpoint"));
    assertEquals(Set.of("tenant_1"), event.payload().get("visibleTenantIds"));
    assertTrue(event.payload().containsKey("capabilities"));
    assertTrue(event.payload().containsKey("limits"));
    assertTrue(event.payload().containsKey("circuitBreakerPolicy"));
  }

  @Test
  void distinguishesUpdatedServiceAuditEventByVersion() {
    InMemoryServiceRegistryAuditPublisher auditPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);

    registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort());
    registry.register(service("agent-1", 2), new FakeDownstreamExecutionPort());

    ServiceRegistryAuditEvent event = auditPublisher.snapshot().get(1);
    assertEquals("service.updated", event.action());
    assertEquals("service-registry-agent-1-v2", event.eventId());
    assertEquals(2, event.version());
    assertEquals(2, registry.find("agent-1").orElseThrow().version());
  }

  @Test
  void doesNotChangeRegistryWhenAuditPublishFails() {
    ServiceRegistryAuditPublisher failingPublisher =
        event -> CompletableFuture.failedFuture(new IllegalStateException("audit unavailable"));
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(failingPublisher, () -> NOW);

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort()));

    assertEquals("audit unavailable", error.getCause().getMessage());
    assertTrue(registry.find("agent-1").isEmpty());
  }

  @Test
  void reloadsRegistrySnapshotAndPublishesChangeAuditEvents() {
    InMemoryServiceRegistryAuditPublisher auditPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);
    FakeDownstreamExecutionPort adapter = new FakeDownstreamExecutionPort();

    registry.register(service("removed-agent", 1), adapter);
    registry.register(service("updated-agent", 1), adapter);

    registry.reload(
        List.of(
            new ServiceRegistryEntry(service("updated-agent", 2), adapter),
            new ServiceRegistryEntry(service("created-agent", 1), adapter)));

    assertTrue(registry.find("removed-agent").isEmpty());
    assertEquals(2, registry.find("updated-agent").orElseThrow().version());
    assertEquals(1, registry.find("created-agent").orElseThrow().version());
    assertEquals(
        List.of("service.updated", "service.registered", "service.removed"),
        auditPublisher.snapshot().stream().skip(2).map(ServiceRegistryAuditEvent::action).toList());
    assertEquals(
        "service-registry-removed-agent-removed-v1", auditPublisher.snapshot().get(4).eventId());
  }

  @Test
  void reloadRejectsDuplicateServiceIdsBeforeChangingSnapshot() {
    InMemoryServiceRegistryAuditPublisher auditPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);
    FakeDownstreamExecutionPort adapter = new FakeDownstreamExecutionPort();

    registry.register(service("agent-1", 1), adapter);
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                registry.reload(
                    List.of(
                        new ServiceRegistryEntry(service("agent-2", 1), adapter),
                        new ServiceRegistryEntry(service("agent-2", 2), adapter))));

    assertEquals("duplicate serviceId: agent-2", error.getMessage());
    assertTrue(registry.find("agent-1").isPresent());
    assertEquals(1, auditPublisher.snapshot().size());
  }

  @Test
  void reloadDoesNotChangeRegistryWhenAuditPublishFails() {
    AtomicInteger publishAttempts = new AtomicInteger();
    ServiceRegistryAuditPublisher failingPublisher =
        event -> {
          if (publishAttempts.incrementAndGet() == 1) {
            return CompletableFuture.completedFuture(null);
          }
          return CompletableFuture.failedFuture(new IllegalStateException("audit unavailable"));
        };
    FakeDownstreamExecutionPort adapter = new FakeDownstreamExecutionPort();

    InMemoryServiceRegistry failingRegistry =
        new InMemoryServiceRegistry(failingPublisher, () -> NOW);
    failingRegistry.register(service("agent-1", 1), adapter);

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () ->
                failingRegistry.reload(
                    List.of(new ServiceRegistryEntry(service("agent-2", 1), adapter))));

    assertEquals("audit unavailable", error.getCause().getMessage());
    assertTrue(failingRegistry.find("agent-1").isPresent());
    assertTrue(failingRegistry.find("agent-2").isEmpty());
  }

  @Test
  void inMemoryAuditPublisherKeepsConfiguredBound() {
    InMemoryServiceRegistryAuditPublisher auditPublisher =
        new InMemoryServiceRegistryAuditPublisher(1);

    auditPublisher.publish(
        ServiceRegistryAuditEvent.fromDescriptor("service.registered", service("a", 1), NOW));
    auditPublisher.publish(
        ServiceRegistryAuditEvent.fromDescriptor("service.registered", service("b", 1), NOW));

    assertEquals(1, auditPublisher.snapshot().size());
    assertEquals("b", auditPublisher.snapshot().getFirst().serviceId());
  }

  @Test
  void sideEventAuditPublisherFanOutsServiceRegistryAuditAfterPrimaryPublish() {
    InMemoryServiceRegistryAuditPublisher primaryPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemorySideEventPublisher sideEventPublisher = new InMemorySideEventPublisher(10);
    ServiceRegistrySideEventAuditPublisher auditPublisher =
        new ServiceRegistrySideEventAuditPublisher(
            primaryPublisher,
            new SideEventDispatcher(
                sideEventPublisher, SideEventDispatchPolicy.failFastAuditForTenants(Set.of())));
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);

    registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort());

    ServiceRegistryAuditEvent auditEvent = primaryPublisher.snapshot().getFirst();
    SideEvent sideEvent = sideEventPublisher.snapshot().getFirst();
    assertEquals(auditEvent.eventId(), sideEvent.eventId());
    assertEquals(ServiceRegistrySideEventAuditPublisher.TOPIC, sideEvent.topic());
    assertEquals("service-registry", sideEvent.runId());
    assertEquals("system", sideEvent.tenantId());
    assertEquals("service.registered", sideEvent.payload().get("action"));
    assertEquals("agent-1", sideEvent.payload().get("serviceId"));
    assertEquals(1L, sideEvent.payload().get("version"));
    assertTrue(((java.util.Map<?, ?>) sideEvent.payload().get("service")).containsKey("endpoint"));
  }

  @Test
  void sideEventAuditPublisherKeepsRegistryChangeBlockedWhenPrimaryAuditFails() {
    ServiceRegistryAuditPublisher primaryPublisher =
        event -> CompletableFuture.failedFuture(new IllegalStateException("primary audit failed"));
    InMemorySideEventPublisher sideEventPublisher = new InMemorySideEventPublisher(10);
    ServiceRegistrySideEventAuditPublisher auditPublisher =
        new ServiceRegistrySideEventAuditPublisher(
            primaryPublisher,
            new SideEventDispatcher(
                sideEventPublisher, SideEventDispatchPolicy.failFastAuditForTenants(Set.of())));
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort()));

    assertEquals("primary audit failed", error.getCause().getMessage());
    assertTrue(registry.find("agent-1").isEmpty());
    assertTrue(sideEventPublisher.snapshot().isEmpty());
  }

  @Test
  void sideEventAuditPublisherRecordsSideFailureWithoutBlockingRegistryChange() {
    InMemoryServiceRegistryAuditPublisher primaryPublisher =
        new InMemoryServiceRegistryAuditPublisher(10);
    InMemorySideEventOutbox sideEventOutbox = new InMemorySideEventOutbox(10);
    ServiceRegistrySideEventAuditPublisher auditPublisher =
        new ServiceRegistrySideEventAuditPublisher(
            primaryPublisher,
            new SideEventDispatcher(
                event -> CompletableFuture.failedFuture(new IllegalStateException("side failed")),
                SideEventDispatchPolicy.failFastAuditForTenants(Set.of()),
                sideEventOutbox,
                java.time.Clock.fixed(NOW, java.time.ZoneOffset.UTC)));
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, () -> NOW);

    registry.register(service("agent-1", 1), new FakeDownstreamExecutionPort());

    assertEquals(1, primaryPublisher.snapshot().size());
    assertTrue(registry.find("agent-1").isPresent());
    assertEquals(1, sideEventOutbox.snapshot().size());
    assertEquals(
        ServiceRegistrySideEventAuditPublisher.TOPIC,
        sideEventOutbox.snapshot().getFirst().topic());
  }

  private static ServiceDescriptor service(String serviceId, long version) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, false, false, true),
        version);
  }

  private static final class FakeDownstreamExecutionPort implements DownstreamExecutionPort {

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
