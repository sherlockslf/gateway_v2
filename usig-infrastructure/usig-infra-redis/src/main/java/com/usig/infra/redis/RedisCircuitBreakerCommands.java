package com.usig.infra.redis;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

public interface RedisCircuitBreakerCommands {

  CompletionStage<Boolean> beforeCall(String circuitKey, Instant now, int halfOpenMaxProbeRuns);

  CompletionStage<Void> recordSuccess(String circuitKey);

  CompletionStage<Void> recordFailure(
      String circuitKey,
      int failureThreshold,
      Instant now,
      Duration openDuration,
      Duration stateTtl);
}
