package com.usig.infra.redis;

import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Request;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public final class QuarkusRedisConcurrencyCommands implements RedisConcurrencyCommands {

  private static final String ACQUIRE_SCRIPT =
      """
      local service = tonumber(redis.call('GET', KEYS[1]) or '0')
      local group = tonumber(redis.call('GET', KEYS[2]) or '0')
      local tenant = tonumber(redis.call('GET', KEYS[3]) or '0')
      if service >= tonumber(ARGV[1]) or group >= tonumber(ARGV[2]) or tenant >= tonumber(ARGV[3]) then
        return 0
      end
      redis.call('INCR', KEYS[1])
      redis.call('INCR', KEYS[2])
      redis.call('INCR', KEYS[3])
      redis.call('EXPIRE', KEYS[1], ARGV[4])
      redis.call('EXPIRE', KEYS[2], ARGV[4])
      redis.call('EXPIRE', KEYS[3], ARGV[4])
      return 1
      """;

  private static final String RELEASE_SCRIPT =
      """
      for i = 1, #KEYS do
        local current = tonumber(redis.call('GET', KEYS[i]) or '0')
        if current <= 1 then
          redis.call('DEL', KEYS[i])
        else
          redis.call('DECR', KEYS[i])
        end
      end
      return 1
      """;

  private final Instance<Redis> redis;

  @Inject
  public QuarkusRedisConcurrencyCommands(Instance<Redis> redis) {
    this.redis = redis;
  }

  @Override
  public CompletionStage<Boolean> acquire(
      String serviceKey,
      String groupKey,
      String tenantServiceKey,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerGroup,
      int maxConcurrentRunsPerTenant,
      Duration ttl) {
    Request request =
        Request.cmd(Command.EVAL)
            .arg(ACQUIRE_SCRIPT)
            .arg(3)
            .arg(serviceKey)
            .arg(groupKey)
            .arg(tenantServiceKey)
            .arg(maxConcurrentRuns)
            .arg(maxConcurrentRunsPerGroup)
            .arg(maxConcurrentRunsPerTenant)
            .arg(ttl.toSeconds());
    return redis
        .get()
        .send(request)
        .map(response -> "1".equals(response.toString()))
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Void> release(
      String serviceKey, String groupKey, String tenantServiceKey) {
    Request request =
        Request.cmd(Command.EVAL)
            .arg(RELEASE_SCRIPT)
            .arg(3)
            .arg(serviceKey)
            .arg(groupKey)
            .arg(tenantServiceKey);
    return redis.get().send(request).replaceWithVoid().subscribeAsCompletionStage();
  }
}
