package com.usig.infra.redis;

import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Request;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public final class QuarkusRedisCircuitBreakerCommands implements RedisCircuitBreakerCommands {

  private static final String BEFORE_CALL_SCRIPT =
      """
      local open_until = tonumber(redis.call('HGET', KEYS[1], 'openUntil') or '0')
      if open_until <= 0 then
        return 1
      end
      if tonumber(ARGV[1]) < open_until then
        return 0
      end
      local probes = tonumber(redis.call('HGET', KEYS[1], 'probes') or '0')
      if probes >= tonumber(ARGV[2]) then
        return 0
      end
      redis.call('HINCRBY', KEYS[1], 'probes', 1)
      return 1
      """;

  private static final String RECORD_FAILURE_SCRIPT =
      """
      local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0') + 1
      local open_until = 0
      if failures >= tonumber(ARGV[1]) then
        open_until = tonumber(ARGV[2]) + tonumber(ARGV[3])
      end
      redis.call('HSET', KEYS[1], 'failures', failures, 'openUntil', open_until, 'probes', 0)
      redis.call('PEXPIRE', KEYS[1], ARGV[4])
      return 1
      """;

  private final Instance<Redis> redis;

  @Inject
  public QuarkusRedisCircuitBreakerCommands(Instance<Redis> redis) {
    this.redis = redis;
  }

  @Override
  public CompletionStage<Boolean> beforeCall(
      String circuitKey, Instant now, int halfOpenMaxProbeRuns) {
    Request request =
        Request.cmd(Command.EVAL)
            .arg(BEFORE_CALL_SCRIPT)
            .arg(1)
            .arg(circuitKey)
            .arg(now.toEpochMilli())
            .arg(halfOpenMaxProbeRuns);
    return redis
        .get()
        .send(request)
        .map(response -> "1".equals(response.toString()))
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Void> recordSuccess(String circuitKey) {
    Request request = Request.cmd(Command.DEL).arg(circuitKey);
    return redis.get().send(request).replaceWithVoid().subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Void> recordFailure(
      String circuitKey,
      int failureThreshold,
      Instant now,
      Duration openDuration,
      Duration stateTtl) {
    Request request =
        Request.cmd(Command.EVAL)
            .arg(RECORD_FAILURE_SCRIPT)
            .arg(1)
            .arg(circuitKey)
            .arg(failureThreshold)
            .arg(now.toEpochMilli())
            .arg(openDuration.toMillis())
            .arg(stateTtl.toMillis());
    return redis.get().send(request).replaceWithVoid().subscribeAsCompletionStage();
  }
}
