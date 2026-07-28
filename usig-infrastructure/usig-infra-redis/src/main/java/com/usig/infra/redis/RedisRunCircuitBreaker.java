package com.usig.infra.redis;

import com.usig.application.port.ClockPort;
import com.usig.application.port.RunCircuitBreaker;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;

public final class RedisRunCircuitBreaker implements RunCircuitBreaker {

  private static final Duration DEFAULT_STATE_TTL = Duration.ofMinutes(10);

  private final RedisCircuitBreakerCommands commands;
  private final ClockPort clock;
  private final String keyPrefix;
  private final Duration minimumStateTtl;

  public RedisRunCircuitBreaker(
      RedisCircuitBreakerCommands commands, ClockPort clock, String keyPrefix) {
    this(commands, clock, keyPrefix, DEFAULT_STATE_TTL);
  }

  public RedisRunCircuitBreaker(
      RedisCircuitBreakerCommands commands,
      ClockPort clock,
      String keyPrefix,
      Duration minimumStateTtl) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.keyPrefix = normalizePrefix(keyPrefix);
    this.minimumStateTtl =
        Objects.requireNonNull(minimumStateTtl, "minimumStateTtl must not be null");
    if (minimumStateTtl.isNegative() || minimumStateTtl.isZero()) {
      throw new IllegalArgumentException("minimumStateTtl must be positive");
    }
  }

  @Override
  public void beforeCall(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    boolean allowed =
        commands
            .beforeCall(
                circuitKey(service.isolationGroupKey()),
                clock.now(),
                service.circuitBreakerPolicy().halfOpenMaxProbeRuns())
            .toCompletableFuture()
            .join();
    if (!allowed) {
      throw new UseCaseException(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN);
    }
  }

  @Override
  public void onSuccess(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    commands.recordSuccess(circuitKey(service.isolationGroupKey())).toCompletableFuture().join();
  }

  @Override
  public void onFailure(ServiceDescriptor service, Throwable throwable) {
    Objects.requireNonNull(service, "service must not be null");
    Duration openDuration = service.circuitBreakerPolicy().openDuration();
    commands
        .recordFailure(
            circuitKey(service.isolationGroupKey()),
            service.circuitBreakerPolicy().failureThreshold(),
            clock.now(),
            openDuration,
            stateTtl(openDuration))
        .toCompletableFuture()
        .join();
  }

  private Duration stateTtl(Duration openDuration) {
    Duration openStateTtl = openDuration.plus(openDuration);
    return openStateTtl.compareTo(minimumStateTtl) > 0 ? openStateTtl : minimumStateTtl;
  }

  private String circuitKey(String isolationGroupKey) {
    return keyPrefix + ":circuit:" + isolationGroupKey;
  }

  private static String normalizePrefix(String keyPrefix) {
    if (keyPrefix == null || keyPrefix.isBlank()) {
      return "usig";
    }
    return keyPrefix;
  }
}
