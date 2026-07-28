package com.usig.infra.redis;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public interface RedisConcurrencyCommands {

  CompletionStage<Boolean> acquire(
      String serviceKey,
      String groupKey,
      String tenantServiceKey,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerGroup,
      int maxConcurrentRunsPerTenant,
      Duration ttl);

  CompletionStage<Void> release(String serviceKey, String groupKey, String tenantServiceKey);
}
