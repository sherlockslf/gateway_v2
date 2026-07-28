package com.usig.application.sideevent;

import java.time.Duration;
import java.util.Objects;

public record SideEventOutboxDrainPolicy(int maxBatchSize, Duration retryDelay) {

  public SideEventOutboxDrainPolicy {
    if (maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be positive");
    }
    retryDelay = Objects.requireNonNull(retryDelay, "retryDelay must not be null");
    if (retryDelay.isNegative()) {
      throw new IllegalArgumentException("retryDelay must not be negative");
    }
  }

  public static SideEventOutboxDrainPolicy defaults() {
    return new SideEventOutboxDrainPolicy(32, Duration.ofSeconds(30));
  }
}
