package com.usig.infra.redis;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.port.ClockPort;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RedisRunCircuitBreakerTest {

  @Test
  void opensDistributedCircuitUntilConfiguredDurationExpires() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T10:00:00Z"));
    RecordingRedisCircuitBreakerCommands commands = new RecordingRedisCircuitBreakerCommands();
    RedisRunCircuitBreaker circuitBreaker =
        new RedisRunCircuitBreaker(commands, clock(now), "usig-test", Duration.ofMinutes(1));
    ServiceDescriptor service =
        service("agent-a", null, new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10), 1));

    circuitBreaker.onFailure(service, new RuntimeException("boom"));

    now.set(Instant.parse("2026-07-27T10:00:09Z"));
    UseCaseException exception =
        assertThrows(UseCaseException.class, () -> circuitBreaker.beforeCall(service));
    assertEquals(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN, exception.errorCode());

    now.set(Instant.parse("2026-07-27T10:00:10Z"));
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
    assertEquals("usig-test:circuit:agent-a", commands.lastCircuitKey);
  }

  @Test
  void limitsHalfOpenProbeRunsUntilProbeSucceeds() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T10:00:00Z"));
    RedisRunCircuitBreaker circuitBreaker =
        new RedisRunCircuitBreaker(
            new RecordingRedisCircuitBreakerCommands(), clock(now), "usig-test");
    ServiceDescriptor service =
        service("agent-a", null, new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10), 1));

    circuitBreaker.onFailure(service, new RuntimeException("boom"));
    now.set(Instant.parse("2026-07-27T10:00:10Z"));

    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
    assertThrows(UseCaseException.class, () -> circuitBreaker.beforeCall(service));

    circuitBreaker.onSuccess(service);
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
  }

  @Test
  void sharesCircuitStateAcrossServicesInSameServiceGroup() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T10:00:00Z"));
    RedisRunCircuitBreaker circuitBreaker =
        new RedisRunCircuitBreaker(
            new RecordingRedisCircuitBreakerCommands(), clock(now), "usig-test");
    ServiceDescriptor first =
        service("agent-a", "group-a", new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10)));
    ServiceDescriptor second =
        service("agent-b", "group-a", new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(10)));

    circuitBreaker.onFailure(first, new RuntimeException("boom"));

    UseCaseException exception =
        assertThrows(UseCaseException.class, () -> circuitBreaker.beforeCall(second));
    assertEquals(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN, exception.errorCode());
  }

  @Test
  void keepsFailureCountBelowThresholdAndClearsOnSuccess() {
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-07-27T10:00:00Z"));
    RedisRunCircuitBreaker circuitBreaker =
        new RedisRunCircuitBreaker(
            new RecordingRedisCircuitBreakerCommands(), clock(now), "usig-test");
    ServiceDescriptor service =
        service("agent-a", null, new ServiceCircuitBreakerPolicy(2, Duration.ofSeconds(10)));

    circuitBreaker.onFailure(service, new RuntimeException("first"));
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));

    circuitBreaker.onSuccess(service);
    circuitBreaker.onFailure(service, new RuntimeException("second"));
    assertDoesNotThrow(() -> circuitBreaker.beforeCall(service));
  }

  private static ClockPort clock(AtomicReference<Instant> now) {
    return now::get;
  }

  private static ServiceDescriptor service(
      String serviceId, String serviceGroup, ServiceCircuitBreakerPolicy circuitBreakerPolicy) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, false, true, true),
        serviceGroup,
        ServiceLimits.defaults(),
        circuitBreakerPolicy,
        1);
  }

  private static final class RecordingRedisCircuitBreakerCommands
      implements RedisCircuitBreakerCommands {

    private final Map<String, State> states = new HashMap<>();
    private String lastCircuitKey;

    @Override
    public CompletionStage<Boolean> beforeCall(
        String circuitKey, Instant now, int halfOpenMaxProbeRuns) {
      lastCircuitKey = circuitKey;
      State state = states.get(circuitKey);
      if (state == null || state.openUntil == null) {
        return CompletableFuture.completedFuture(true);
      }
      if (now.isBefore(state.openUntil)) {
        return CompletableFuture.completedFuture(false);
      }
      if (state.probes >= halfOpenMaxProbeRuns) {
        return CompletableFuture.completedFuture(false);
      }
      states.put(circuitKey, new State(state.failures, state.openUntil, state.probes + 1));
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletionStage<Void> recordSuccess(String circuitKey) {
      states.remove(circuitKey);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> recordFailure(
        String circuitKey,
        int failureThreshold,
        Instant now,
        Duration openDuration,
        Duration stateTtl) {
      State current = states.getOrDefault(circuitKey, new State(0, null, 0));
      int failures = current.failures + 1;
      Instant openUntil = failures >= failureThreshold ? now.plus(openDuration) : null;
      states.put(circuitKey, new State(failures, openUntil, 0));
      return CompletableFuture.completedFuture(null);
    }

    private record State(int failures, Instant openUntil, int probes) {}
  }
}
