package com.usig.infra.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PostgresServiceDescriptorRecordTest {

  @Test
  void roundTripsServiceDescriptorWithOperationalPolicyFields() {
    ServiceDescriptor service =
        new ServiceDescriptor(
            "agent-1",
            ServiceType.AGENT,
            URI.create("https://internal.example/agent-1"),
            "agent-a2a-v1",
            Set.of("tenant_1", "tenant_2"),
            Set.of(Transport.SSE, Transport.WEBSOCKET),
            new ServiceCapabilities(
                true, true, true, true, true, true, true, true, RetrySafety.RESUME_SAFE),
            "agent-group",
            new ServiceLimits(200, 50, 150),
            new ServiceCircuitBreakerPolicy(7, Duration.ofSeconds(45), 2),
            Set.of("Images.Example.COM", "cdn.example.com"),
            3);

    PostgresServiceDescriptorRecord record = PostgresServiceDescriptorRecord.fromDomain(service);
    ServiceDescriptor restored = record.toDomain();

    assertEquals("AGENT", record.serviceType());
    assertEquals("agent-a2a-v1", record.adapterId());
    assertEquals(Set.of("SSE", "WEBSOCKET"), record.transports());
    assertEquals("RESUME_SAFE", record.capabilities().get("retrySafety"));
    assertEquals("PT45S", record.circuitBreakerPolicy().get("openDuration"));
    assertEquals(service, restored);
    assertEquals(Set.of("images.example.com", "cdn.example.com"), restored.allowedImageHosts());
  }

  @Test
  void mapsPartialJsonRecordUsingDomainDefaults() {
    PostgresServiceDescriptorRecord record =
        new PostgresServiceDescriptorRecord(
            "model-chat",
            "MODEL",
            "https://models.internal.example/v1/responses",
            "openai-compatible-v1",
            Set.of(),
            Set.of("SSE"),
            Map.of("streaming", "true", "cancel", true, "retrySafety", "NONE"),
            " ",
            Map.of(),
            Map.of(),
            Set.of(),
            1);

    ServiceDescriptor service = record.toDomain();

    assertEquals(ServiceType.MODEL, service.serviceType());
    assertEquals(URI.create("https://models.internal.example/v1/responses"), service.endpoint());
    assertEquals(Set.of(Transport.SSE), service.transports());
    assertEquals(ServiceLimits.defaults(), service.limits());
    assertEquals(ServiceCircuitBreakerPolicy.defaults(), service.circuitBreakerPolicy());
    assertEquals(null, service.serviceGroup());
    assertEquals(RetrySafety.NONE, service.capabilities().retrySafety());
  }

  @Test
  void rejectsNonPositiveVersionAtRecordBoundary() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PostgresServiceDescriptorRecord(
                    "agent-1",
                    "AGENT",
                    "https://internal.example/agent-1",
                    "agent-a2a-v1",
                    Set.of(),
                    Set.of("SSE"),
                    Map.of(),
                    null,
                    Map.of(),
                    Map.of(),
                    Set.of(),
                    0));

    assertEquals("version must be positive", error.getMessage());
  }
}
