package com.usig.bootstrap;

import java.time.Duration;
import java.util.Objects;

public record ServiceRegistryReloadWorkerConfig(boolean enabled, Duration interval) {

  public ServiceRegistryReloadWorkerConfig {
    Objects.requireNonNull(interval, "interval");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("interval must be positive");
    }
  }

  public static ServiceRegistryReloadWorkerConfig defaults() {
    return new ServiceRegistryReloadWorkerConfig(false, Duration.ofSeconds(30));
  }
}
