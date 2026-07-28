package com.usig.application.memory;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InMemoryRunConcurrencyLimiter implements RunConcurrencyLimiter {

  private final Object lock = new Object();
  private final Map<String, Integer> serviceCounts = new HashMap<>();
  private final Map<String, Integer> groupCounts = new HashMap<>();
  private final Map<TenantServiceKey, Integer> tenantServiceCounts = new HashMap<>();

  @Override
  public Permit acquire(TenantId tenantId, ServiceDescriptor service) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    Objects.requireNonNull(service, "service must not be null");
    String serviceId = service.serviceId();
    String groupKey = service.isolationGroupKey();
    TenantServiceKey tenantServiceKey = new TenantServiceKey(tenantId.value(), serviceId);
    synchronized (lock) {
      int serviceCount = serviceCounts.getOrDefault(serviceId, 0);
      int groupCount = groupCounts.getOrDefault(groupKey, 0);
      int tenantServiceCount = tenantServiceCounts.getOrDefault(tenantServiceKey, 0);
      if (serviceCount >= service.limits().maxConcurrentRuns()
          || groupCount >= service.limits().maxConcurrentRunsPerGroup()
          || tenantServiceCount >= service.limits().maxConcurrentRunsPerTenant()) {
        throw new UseCaseException(ErrorCode.TENANT_LIMIT_EXCEEDED);
      }
      serviceCounts.put(serviceId, serviceCount + 1);
      groupCounts.put(groupKey, groupCount + 1);
      tenantServiceCounts.put(tenantServiceKey, tenantServiceCount + 1);
    }
    return new InMemoryPermit(serviceId, groupKey, tenantServiceKey);
  }

  private void release(String serviceId, String groupKey, TenantServiceKey tenantServiceKey) {
    synchronized (lock) {
      decrement(serviceCounts, serviceId);
      decrement(groupCounts, groupKey);
      decrement(tenantServiceCounts, tenantServiceKey);
    }
  }

  private static <K> void decrement(Map<K, Integer> counts, K key) {
    int next = counts.getOrDefault(key, 0) - 1;
    if (next <= 0) {
      counts.remove(key);
    } else {
      counts.put(key, next);
    }
  }

  private record TenantServiceKey(String tenantId, String serviceId) {}

  private final class InMemoryPermit implements Permit {

    private final String serviceId;
    private final String groupKey;
    private final TenantServiceKey tenantServiceKey;
    private final AtomicBoolean closed = new AtomicBoolean();

    private InMemoryPermit(String serviceId, String groupKey, TenantServiceKey tenantServiceKey) {
      this.serviceId = serviceId;
      this.groupKey = groupKey;
      this.tenantServiceKey = tenantServiceKey;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        release(serviceId, groupKey, tenantServiceKey);
      }
    }
  }
}
