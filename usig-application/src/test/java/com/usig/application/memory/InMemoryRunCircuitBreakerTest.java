package com.usig.application.memory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.port.ClockPort;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class InMemoryRunCircuitBreakerTest {

  @Test
  void keepsCircuitOpenUntilConfiguredOpenDurationExpires() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T09:00:00Z"));
    ClockPort clock = now::get;
    InMemoryRunCircuitBreaker circuitBreaker = new InMemoryRunCircuitBreaker(clock);
    ServiceDescriptor service =
        service("agent-short-open", new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10)));

    circuitBreaker.onFailure(service, new RuntimeException("boom"));

    now.set(Instant.parse("2026-07-27T09:00:09Z"));
    assertThrows(UseCaseException.class, () -> circuitBreaker.beforeCall(service));

    now.set(Instant.parse("2026-07-27T09:00:10Z"));
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
  }

  @Test
  void limitsHalfOpenProbeRunsUntilProbeSucceeds() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T09:00:00Z"));
    ClockPort clock = now::get;
    InMemoryRunCircuitBreaker circuitBreaker = new InMemoryRunCircuitBreaker(clock);
    ServiceDescriptor service =
        service("agent-half-open", new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10), 1));

    circuitBreaker.onFailure(service, new RuntimeException("boom"));
    now.set(Instant.parse("2026-07-27T09:00:10Z"));

    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
    assertThrows(UseCaseException.class, () -> circuitBreaker.beforeCall(service));

    circuitBreaker.onSuccess(service);
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
  }

  private static ServiceDescriptor service(
      String serviceId, ServiceCircuitBreakerPolicy circuitBreakerPolicy) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, false, true, true),
        null,
        ServiceLimits.defaults(),
        circuitBreakerPolicy,
        1);
  }
}
