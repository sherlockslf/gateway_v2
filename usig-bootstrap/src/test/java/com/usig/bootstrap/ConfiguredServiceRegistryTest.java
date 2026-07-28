package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.port.ImageUploadCredentialIssuer;
import com.usig.application.port.ImageUploadLifecycleCleaner;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.serviceaudit.InMemoryServiceRegistryAuditPublisher;
import com.usig.application.serviceaudit.ServiceRegistryAuditEvent;
import com.usig.application.serviceaudit.ServiceRegistrySideEventAuditPublisher;
import com.usig.application.sideevent.InMemorySideEventOutbox;
import com.usig.application.sideevent.InMemorySideEventPublisher;
import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventDispatchPolicy;
import com.usig.application.sideevent.SideEventOutboxDrainPolicy;
import com.usig.application.sideevent.SideEventOutboxDrainer;
import com.usig.application.usecase.CreateRunRetryPolicy;
import com.usig.application.usecase.ResumeRetryPolicy;
import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.Transport;
import com.usig.infra.objectstorage.S3CompatibleImageUploadCredentialIssuer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ConfiguredServiceRegistryTest.ConfiguredServiceProfile.class)
class ConfiguredServiceRegistryTest {

  @Inject ServiceRegistry serviceRegistry;
  @Inject InMemoryServiceRegistryAuditPublisher serviceRegistryAuditPublisher;
  @Inject InMemorySideEventPublisher sideEventPublisher;
  @Inject SideEventDispatchPolicy sideEventDispatchPolicy;
  @Inject InMemorySideEventOutbox sideEventOutbox;
  @Inject SideEventOutboxDrainPolicy sideEventOutboxDrainPolicy;
  @Inject SideEventOutboxDrainer sideEventOutboxDrainer;
  @Inject SideEventOutboxDrainWorkerConfig sideEventOutboxDrainWorkerConfig;
  @Inject SideEventOutboxDrainWorker sideEventOutboxDrainWorker;
  @Inject ServiceRegistryReloadWorkerConfig serviceRegistryReloadWorkerConfig;
  @Inject ServiceRegistryReloadWorker serviceRegistryReloadWorker;
  @Inject CreateRunRetryPolicy createRunRetryPolicy;
  @Inject ResumeRetryPolicy resumeRetryPolicy;
  @Inject ImageUploadCredentialIssuer imageUploadCredentialIssuer;
  @Inject ImageUploadLifecycleCleaner imageUploadLifecycleCleaner;

  @Test
  void registersServiceDescriptorFromConfiguration() {
    ServiceDescriptor service = serviceRegistry.find("configured-agent").orElseThrow();

    assertEquals("configured-agent", service.serviceId());
    assertEquals("sample-agent-v1", service.adapterId());
    assertEquals(Set.of("tenant_cfg"), service.visibleTenantIds());
    assertEquals(Set.of(Transport.SSE), service.transports());
    assertEquals("configured-group", service.serviceGroup());
    assertEquals(7, service.limits().maxConcurrentRuns());
    assertEquals(3, service.limits().maxConcurrentRunsPerTenant());
    assertEquals(5, service.limits().maxConcurrentRunsPerGroup());
    assertEquals(2, service.circuitBreakerPolicy().failureThreshold());
    assertEquals(Duration.ofSeconds(11), service.circuitBreakerPolicy().openDuration());
    assertEquals(1, service.circuitBreakerPolicy().halfOpenMaxProbeRuns());
    assertEquals(Set.of("cdn.example.com", "assets.example.com"), service.allowedImageHosts());
    assertTrue(service.capabilities().streaming());
    assertEquals(RetrySafety.IDEMPOTENT_CREATE_ONLY, service.capabilities().retrySafety());
  }

  @Test
  void serviceApiUsesConfiguredRegistry() {
    given()
        .header("X-Tenant-Id", "tenant_cfg")
        .when()
        .get("/v1/services")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].serviceId", equalTo("configured-agent"))
        .body("[0].capabilities.retrySafety", equalTo("IDEMPOTENT_CREATE_ONLY"));

    given()
        .header("X-Tenant-Id", "tenant_cfg")
        .when()
        .get("/v1/services/sample-agent/capabilities")
        .then()
        .statusCode(404)
        .body("code", equalTo("SERVICE_NOT_FOUND"));
  }

  @Test
  void publishesAuditEventForConfiguredServiceRegistration() {
    ServiceRegistryAuditEvent event =
        serviceRegistryAuditPublisher.snapshot().stream()
            .filter(candidate -> "configured-agent".equals(candidate.serviceId()))
            .findFirst()
            .orElseThrow();

    assertEquals("service.registered", event.action());
    assertEquals("configured-agent", event.serviceId());
    assertEquals(1, event.version());
    assertEquals("sample-agent-v1", event.payload().get("adapterId"));
    assertEquals("https://internal.example/configured-agent", event.payload().get("endpoint"));
    assertEquals("configured-group", event.payload().get("serviceGroup"));
    assertEquals(
        Set.of("cdn.example.com", "assets.example.com"), event.payload().get("allowedImageHosts"));
  }

  @Test
  void publishesServiceRegistryAuditToSideEventStream() {
    SideEvent event =
        sideEventPublisher.snapshot().stream()
            .filter(candidate -> "service-registry-configured-agent-v1".equals(candidate.eventId()))
            .findFirst()
            .orElseThrow();

    assertEquals(ServiceRegistrySideEventAuditPublisher.TOPIC, event.topic());
    assertEquals("service-registry", event.runId());
    assertEquals("system", event.tenantId());
    assertEquals("service.registered", event.payload().get("action"));
    assertEquals("configured-agent", event.payload().get("serviceId"));
    assertEquals(1L, event.payload().get("version"));
    assertTrue(((Map<?, ?>) event.payload().get("service")).containsKey("allowedImageHosts"));
  }

  @Test
  void configuresFailFastAuditTenantsForSideEventDispatchPolicy() {
    assertTrue(
        sideEventDispatchPolicy.shouldFailFast(
            new SideEvent(
                "usig.audit.v1",
                "evt_1",
                "run_1",
                "tenant_cfg",
                "trace_1",
                Instant.parse("2026-07-27T12:50:00Z"),
                Map.of())));
    assertTrue(
        !sideEventDispatchPolicy.shouldFailFast(
            new SideEvent(
                "usig.message.persist.v1",
                "evt_2",
                "run_1",
                "tenant_cfg",
                "trace_1",
                Instant.parse("2026-07-27T12:50:00Z"),
                Map.of())));
  }

  @Test
  void configuresBoundedSideEventOutbox() {
    sideEventOutbox
        .recordFailure(sideEvent("tenant_1"), new IllegalStateException("failure-1"), Instant.EPOCH)
        .toCompletableFuture()
        .join();
    sideEventOutbox
        .recordFailure(sideEvent("tenant_2"), new IllegalStateException("failure-2"), Instant.EPOCH)
        .toCompletableFuture()
        .join();
    sideEventOutbox
        .recordFailure(sideEvent("tenant_3"), new IllegalStateException("failure-3"), Instant.EPOCH)
        .toCompletableFuture()
        .join();

    assertEquals(2, sideEventOutbox.snapshot().size());
    assertEquals("tenant_2", sideEventOutbox.snapshot().get(0).tenantId());
    assertEquals("tenant_3", sideEventOutbox.snapshot().get(1).tenantId());
  }

  @Test
  void configuresSideEventOutboxDrainPolicyAndDrainer() {
    assertEquals(2, sideEventOutboxDrainPolicy.maxBatchSize());
    assertEquals(Duration.ofSeconds(7), sideEventOutboxDrainPolicy.retryDelay());
    assertTrue(!sideEventOutboxDrainWorkerConfig.enabled());
    assertEquals(Duration.ofSeconds(13), sideEventOutboxDrainWorkerConfig.interval());
    assertTrue(!sideEventOutboxDrainWorker.isStarted());
    assertTrue(!serviceRegistryReloadWorkerConfig.enabled());
    assertEquals(Duration.ofSeconds(17), serviceRegistryReloadWorkerConfig.interval());
    assertTrue(!serviceRegistryReloadWorker.isStarted());
    sideEventOutbox
        .recordFailure(
            sideEvent("tenant_drain"), new IllegalStateException("failure"), Instant.EPOCH)
        .toCompletableFuture()
        .join();

    assertEquals(1, sideEventOutboxDrainer.drainOnce().toCompletableFuture().join().published());
  }

  @Test
  void configuresCreateRunRetryPolicy() {
    assertEquals(3, createRunRetryPolicy.safeCreateMaxAttempts());
    assertEquals(Duration.ofMillis(25), createRunRetryPolicy.baseDelay());
    assertEquals(Duration.ofMillis(75), createRunRetryPolicy.jitterRange());
  }

  @Test
  void configuresResumeRetryPolicy() {
    assertEquals(4, resumeRetryPolicy.maxAttempts());
    assertEquals(Duration.ofMillis(30), resumeRetryPolicy.baseDelay());
    assertEquals(Duration.ofMillis(90), resumeRetryPolicy.jitterRange());
  }

  @Test
  void exposesImageUploadLifecycleCleaner() {
    assertInstanceOf(S3CompatibleImageUploadCredentialIssuer.class, imageUploadCredentialIssuer);
    assertEquals(
        0,
        imageUploadLifecycleCleaner
            .cleanupExpiredUploads()
            .toCompletableFuture()
            .join()
            .removedUploadRecords());
  }

  public static final class ConfiguredServiceProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("usig.services.ids", "configured-agent"),
          Map.entry("usig.services.configured-agent.service-type", "AGENT"),
          Map.entry(
              "usig.services.configured-agent.endpoint",
              "https://internal.example/configured-agent"),
          Map.entry("usig.services.configured-agent.adapter-id", "sample-agent-v1"),
          Map.entry("usig.services.configured-agent.visible-tenants", "tenant_cfg"),
          Map.entry("usig.services.configured-agent.transports", "SSE"),
          Map.entry("usig.services.configured-agent.service-group", "configured-group"),
          Map.entry(
              "usig.services.configured-agent.allowed-image-hosts",
              "cdn.example.com,assets.example.com"),
          Map.entry("usig.services.configured-agent.capabilities.streaming", "true"),
          Map.entry("usig.services.configured-agent.capabilities.cancel", "true"),
          Map.entry("usig.services.configured-agent.capabilities.resume", "false"),
          Map.entry("usig.services.configured-agent.capabilities.replay", "false"),
          Map.entry("usig.services.configured-agent.capabilities.task-query", "true"),
          Map.entry("usig.services.configured-agent.capabilities.human-input", "false"),
          Map.entry("usig.services.configured-agent.capabilities.multimodal-input", "false"),
          Map.entry("usig.services.configured-agent.capabilities.structured-output", "true"),
          Map.entry(
              "usig.services.configured-agent.capabilities.retry-safety", "IDEMPOTENT_CREATE_ONLY"),
          Map.entry("usig.services.configured-agent.limits.max-concurrent-runs", "7"),
          Map.entry("usig.services.configured-agent.limits.max-concurrent-runs-per-tenant", "3"),
          Map.entry("usig.services.configured-agent.limits.max-concurrent-runs-per-group", "5"),
          Map.entry("usig.side-events.fail-fast-audit-tenants", "tenant_cfg"),
          Map.entry("usig.side-events.outbox.max-entries", "2"),
          Map.entry("usig.side-events.outbox.drain.max-batch-size", "2"),
          Map.entry("usig.side-events.outbox.drain.retry-delay", "PT7S"),
          Map.entry("usig.side-events.outbox.drain.worker.enabled", "false"),
          Map.entry("usig.side-events.outbox.drain.worker.interval", "PT13S"),
          Map.entry("usig.service-registry.reload.source", "none"),
          Map.entry("usig.service-registry.reload.worker.enabled", "false"),
          Map.entry("usig.service-registry.reload.worker.interval", "PT17S"),
          Map.entry("usig.retry.create.safe-max-attempts", "3"),
          Map.entry("usig.retry.create.base-delay", "PT0.025S"),
          Map.entry("usig.retry.create.jitter-range", "PT0.075S"),
          Map.entry("usig.retry.resume.max-attempts", "4"),
          Map.entry("usig.retry.resume.base-delay", "PT0.03S"),
          Map.entry("usig.retry.resume.jitter-range", "PT0.09S"),
          Map.entry("usig.image-upload.backend", "s3-compatible"),
          Map.entry("usig.image-upload.s3.endpoint", "https://s3.example.com"),
          Map.entry("usig.image-upload.s3.public-base-uri", "https://cdn.example.com"),
          Map.entry("usig.image-upload.s3.bucket", "usig-images"),
          Map.entry("usig.image-upload.s3.region", "us-east-1"),
          Map.entry("usig.image-upload.s3.access-key-id", "AKIDEXAMPLE"),
          Map.entry("usig.image-upload.s3.secret-access-key", "SECRET"),
          Map.entry("usig.image-upload.s3.upload-ttl", "PT15M"),
          Map.entry("usig.services.configured-agent.circuit-breaker.failure-threshold", "2"),
          Map.entry("usig.services.configured-agent.circuit-breaker.open-duration", "PT11S"),
          Map.entry(
              "usig.services.configured-agent.circuit-breaker.half-open-max-probe-runs", "1"));
    }
  }

  private static SideEvent sideEvent(String tenantId) {
    return new SideEvent(
        "usig.audit.v1",
        "evt_" + tenantId,
        "run_1",
        tenantId,
        "trace_1",
        Instant.parse("2026-07-27T12:50:00Z"),
        Map.of());
  }
}
