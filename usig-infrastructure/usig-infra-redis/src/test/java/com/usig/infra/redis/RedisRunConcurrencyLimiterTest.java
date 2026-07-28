package com.usig.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class RedisRunConcurrencyLimiterTest {

  @Test
  void acquiresAndReleasesDistributedCounters() {
    RecordingRedisConcurrencyCommands commands = new RecordingRedisConcurrencyCommands();
    RedisRunConcurrencyLimiter limiter =
        new RedisRunConcurrencyLimiter(commands, "usig-test", Duration.ofMinutes(2));

    RunConcurrencyLimiter.Permit permit =
        limiter.acquire(new TenantId("tenant_1"), service("agent-a", "group-a", 2, 1, 2));

    assertEquals("usig-test:concurrency:service:agent-a", commands.lastServiceKey);
    assertEquals("usig-test:concurrency:group:group-a", commands.lastGroupKey);
    assertEquals("usig-test:concurrency:tenant:tenant_1:service:agent-a", commands.lastTenantKey);
    assertEquals(Duration.ofMinutes(2), commands.lastTtl);
    assertEquals(1, commands.count(commands.lastServiceKey));

    permit.close();
    permit.close();

    assertEquals(0, commands.count("usig-test:concurrency:service:agent-a"));
    assertEquals(1, commands.releaseCalls);
  }

  @Test
  void rejectsWhenServiceLimitIsReached() {
    RedisRunConcurrencyLimiter limiter =
        new RedisRunConcurrencyLimiter(new RecordingRedisConcurrencyCommands(), "usig-test");
    ServiceDescriptor service = service("agent-a", "group-a", 1, 1, 5);

    limiter.acquire(new TenantId("tenant_1"), service);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class, () -> limiter.acquire(new TenantId("tenant_2"), service));
    assertEquals(ErrorCode.TENANT_LIMIT_EXCEEDED, exception.errorCode());
  }

  @Test
  void rejectsWhenTenantServiceLimitIsReached() {
    RedisRunConcurrencyLimiter limiter =
        new RedisRunConcurrencyLimiter(new RecordingRedisConcurrencyCommands(), "usig-test");
    ServiceDescriptor service = service("agent-a", "group-a", 5, 1, 5);

    limiter.acquire(new TenantId("tenant_1"), service);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class, () -> limiter.acquire(new TenantId("tenant_1"), service));
    assertEquals(ErrorCode.TENANT_LIMIT_EXCEEDED, exception.errorCode());
  }

  @Test
  void rejectsWhenServiceGroupLimitIsReachedAcrossServices() {
    RedisRunConcurrencyLimiter limiter =
        new RedisRunConcurrencyLimiter(new RecordingRedisConcurrencyCommands(), "usig-test");

    limiter.acquire(new TenantId("tenant_1"), service("agent-a", "group-a", 5, 5, 1));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                limiter.acquire(new TenantId("tenant_2"), service("agent-b", "group-a", 5, 5, 1)));
    assertEquals(ErrorCode.TENANT_LIMIT_EXCEEDED, exception.errorCode());
  }

  private static ServiceDescriptor service(
      String serviceId,
      String serviceGroup,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerTenant,
      int maxConcurrentRunsPerGroup) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1", "tenant_2"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, false, true, true),
        serviceGroup,
        new ServiceLimits(maxConcurrentRuns, maxConcurrentRunsPerTenant, maxConcurrentRunsPerGroup),
        1);
  }

  private static final class RecordingRedisConcurrencyCommands implements RedisConcurrencyCommands {

    private final Map<String, Integer> counts = new HashMap<>();
    private String lastServiceKey;
    private String lastGroupKey;
    private String lastTenantKey;
    private Duration lastTtl;
    private int releaseCalls;

    @Override
    public CompletionStage<Boolean> acquire(
        String serviceKey,
        String groupKey,
        String tenantServiceKey,
        int maxConcurrentRuns,
        int maxConcurrentRunsPerGroup,
        int maxConcurrentRunsPerTenant,
        Duration ttl) {
      lastServiceKey = serviceKey;
      lastGroupKey = groupKey;
      lastTenantKey = tenantServiceKey;
      lastTtl = ttl;
      if (count(serviceKey) >= maxConcurrentRuns
          || count(groupKey) >= maxConcurrentRunsPerGroup
          || count(tenantServiceKey) >= maxConcurrentRunsPerTenant) {
        return CompletableFuture.completedFuture(false);
      }
      increment(serviceKey);
      increment(groupKey);
      increment(tenantServiceKey);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletionStage<Void> release(
        String serviceKey, String groupKey, String tenantServiceKey) {
      releaseCalls++;
      decrement(serviceKey);
      decrement(groupKey);
      decrement(tenantServiceKey);
      return CompletableFuture.completedFuture(null);
    }

    private int count(String key) {
      return counts.getOrDefault(key, 0);
    }

    private void increment(String key) {
      counts.put(key, count(key) + 1);
    }

    private void decrement(String key) {
      int next = count(key) - 1;
      if (next <= 0) {
        counts.remove(key);
      } else {
        counts.put(key, next);
      }
    }
  }
}
