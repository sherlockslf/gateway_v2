package com.usig.domain.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ServiceDescriptorTest {

  @Test
  void exposesCapabilityDrivenServiceDescription() {
    ServiceDescriptor descriptor =
        new ServiceDescriptor(
            "finance-agent",
            ServiceType.AGENT,
            URI.create("https://internal.example/agent"),
            "sample-agent-v1",
            Set.of("tenant_1"),
            Set.of(Transport.SSE),
            new ServiceCapabilities(
                true, true, true, false, true, true, true, true, RetrySafety.RESUME_SAFE),
            1);

    assertTrue(descriptor.supportsTransport(Transport.SSE));
    assertFalse(descriptor.supportsTransport(Transport.WEBSOCKET));
    assertTrue(descriptor.capabilities().cancel());
    assertEquals(RetrySafety.RESUME_SAFE, descriptor.capabilities().retrySafety());
    assertTrue(descriptor.visibleToTenant("tenant_1"));
    assertFalse(descriptor.visibleToTenant("tenant_2"));
  }

  @Test
  void normalizesAllowedImageHosts() {
    ServiceDescriptor descriptor =
        new ServiceDescriptor(
            "vision-agent",
            ServiceType.AGENT,
            URI.create("https://internal.example/vision"),
            "sample-agent-v1",
            Set.of("tenant_1"),
            Set.of(Transport.SSE),
            new ServiceCapabilities(true, true, false, false, true, true, true, true),
            null,
            ServiceLimits.defaults(),
            ServiceCircuitBreakerPolicy.defaults(),
            Set.of(" CDN.EXAMPLE.COM ", "assets.example.com"),
            1);

    assertEquals(Set.of("cdn.example.com", "assets.example.com"), descriptor.allowedImageHosts());
  }
}
