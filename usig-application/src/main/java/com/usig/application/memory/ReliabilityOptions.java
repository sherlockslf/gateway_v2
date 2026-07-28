package com.usig.application.memory;

import java.time.Duration;
import java.util.List;

public record ReliabilityOptions(
    int maxPendingEvents,
    int maxPendingBytes,
    Duration replayTtl,
    boolean coalesceTextDelta,
    List<String> redactionLiterals) {

  public ReliabilityOptions {
    if (maxPendingEvents < 1) {
      throw new IllegalArgumentException("maxPendingEvents must be positive");
    }
    if (maxPendingBytes < 1) {
      throw new IllegalArgumentException("maxPendingBytes must be positive");
    }
    if (replayTtl == null || replayTtl.isZero() || replayTtl.isNegative()) {
      throw new IllegalArgumentException("replayTtl must be positive");
    }
    redactionLiterals = List.copyOf(redactionLiterals == null ? List.of() : redactionLiterals);
  }

  public static ReliabilityOptions defaults() {
    return new ReliabilityOptions(256, 1024 * 1024, Duration.ofMinutes(10), true, List.of());
  }
}
