package com.usig.infra.redis;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RedisRunConcurrencyLimiter implements RunConcurrencyLimiter {

  private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);

  private final RedisConcurrencyCommands commands;
  private final String keyPrefix;
  private final Duration ttl;

  public RedisRunConcurrencyLimiter(RedisConcurrencyCommands commands, String keyPrefix) {
    this(commands, keyPrefix, DEFAULT_TTL);
  }

  public RedisRunConcurrencyLimiter(
      RedisConcurrencyCommands commands, String keyPrefix, Duration ttl) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.keyPrefix = normalizePrefix(keyPrefix);
    this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
    if (ttl.isNegative() || ttl.isZero()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
  }

  @Override
  public Permit acquire(TenantId tenantId, ServiceDescriptor service) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    Objects.requireNonNull(service, "service must not be null");
    String serviceKey = serviceKey(service.serviceId());
    String groupKey = groupKey(service.isolationGroupKey());
    String tenantServiceKey = tenantServiceKey(tenantId.value(), service.serviceId());
    boolean acquired =
        commands
            .acquire(
                serviceKey,
                groupKey,
                tenantServiceKey,
                service.limits().maxConcurrentRuns(),
                service.limits().maxConcurrentRunsPerGroup(),
                service.limits().maxConcurrentRunsPerTenant(),
                ttl)
            .toCompletableFuture()
            .join();
    if (!acquired) {
      throw new UseCaseException(ErrorCode.TENANT_LIMIT_EXCEEDED);
    }
    return new RedisPermit(serviceKey, groupKey, tenantServiceKey);
  }

  private String serviceKey(String serviceId) {
    return keyPrefix + ":concurrency:service:" + serviceId;
  }

  private String groupKey(String groupId) {
    return keyPrefix + ":concurrency:group:" + groupId;
  }

  private String tenantServiceKey(String tenantId, String serviceId) {
    return keyPrefix + ":concurrency:tenant:" + tenantId + ":service:" + serviceId;
  }

  private static String normalizePrefix(String keyPrefix) {
    if (keyPrefix == null || keyPrefix.isBlank()) {
      return "usig";
    }
    return keyPrefix;
  }

  private final class RedisPermit implements Permit {

    private final String serviceKey;
    private final String groupKey;
    private final String tenantServiceKey;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RedisPermit(String serviceKey, String groupKey, String tenantServiceKey) {
      this.serviceKey = serviceKey;
      this.groupKey = groupKey;
      this.tenantServiceKey = tenantServiceKey;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        commands.release(serviceKey, groupKey, tenantServiceKey).toCompletableFuture().join();
      }
    }
  }
}
