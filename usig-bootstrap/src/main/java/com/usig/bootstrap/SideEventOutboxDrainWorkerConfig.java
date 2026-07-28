package com.usig.bootstrap;

import java.time.Duration;
import java.util.Objects;

public record SideEventOutboxDrainWorkerConfig(boolean enabled, Duration interval) {

  public SideEventOutboxDrainWorkerConfig {
    Objects.requireNonNull(interval, "interval");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("interval must be positive");
    }
  }

  public static SideEventOutboxDrainWorkerConfig defaults() {
    return new SideEventOutboxDrainWorkerConfig(false, Duration.ofSeconds(30));
  }
}
