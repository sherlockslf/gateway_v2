package com.usig.domain.service;

import java.time.Duration;
import java.util.Objects;

public record ServiceCircuitBreakerPolicy(
    int failureThreshold, Duration openDuration, int halfOpenMaxProbeRuns) {

  private static final ServiceCircuitBreakerPolicy DEFAULTS =
      new ServiceCircuitBreakerPolicy(5, Duration.ofSeconds(30), 1);

  public ServiceCircuitBreakerPolicy(int failureThreshold, Duration openDuration) {
    this(failureThreshold, openDuration, 1);
  }

  public ServiceCircuitBreakerPolicy {
    if (failureThreshold < 1) {
      throw new IllegalArgumentException("failureThreshold must be positive");
    }
    openDuration = Objects.requireNonNull(openDuration, "openDuration must not be null");
    if (openDuration.isNegative() || openDuration.isZero()) {
      throw new IllegalArgumentException("openDuration must be positive");
    }
    if (halfOpenMaxProbeRuns < 1) {
      throw new IllegalArgumentException("halfOpenMaxProbeRuns must be positive");
    }
  }

  public static ServiceCircuitBreakerPolicy defaults() {
    return DEFAULTS;
  }
}
