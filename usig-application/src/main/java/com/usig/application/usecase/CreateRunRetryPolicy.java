package com.usig.application.usecase;

import com.usig.domain.service.RetrySafety;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public record CreateRunRetryPolicy(
    int safeCreateMaxAttempts, Duration baseDelay, Duration jitterRange) {

  public static CreateRunRetryPolicy defaults() {
    return new CreateRunRetryPolicy(2, Duration.ofMillis(10), Duration.ofMillis(40));
  }

  public static CreateRunRetryPolicy immediateForTests() {
    return new CreateRunRetryPolicy(2, Duration.ZERO, Duration.ZERO);
  }

  public CreateRunRetryPolicy {
    if (safeCreateMaxAttempts < 1) {
      throw new IllegalArgumentException("safeCreateMaxAttempts must be at least 1");
    }
    Objects.requireNonNull(baseDelay, "baseDelay must not be null");
    Objects.requireNonNull(jitterRange, "jitterRange must not be null");
    if (baseDelay.isNegative()) {
      throw new IllegalArgumentException("baseDelay must not be negative");
    }
    if (jitterRange.isNegative()) {
      throw new IllegalArgumentException("jitterRange must not be negative");
    }
  }

  int maxAttemptsFor(RetrySafety retrySafety) {
    return retrySafety == RetrySafety.NONE ? 1 : safeCreateMaxAttempts;
  }

  Duration delayBeforeRetry(int failedAttempt) {
    long multiplier = Math.max(1, failedAttempt);
    long baseMillis = safeMultiply(baseDelay.toMillis(), multiplier);
    long jitterMillis =
        jitterRange.isZero() ? 0 : ThreadLocalRandom.current().nextLong(jitterRange.toMillis() + 1);
    return Duration.ofMillis(safeAdd(baseMillis, jitterMillis));
  }

  private static long safeMultiply(long value, long multiplier) {
    if (value > 0 && multiplier > Long.MAX_VALUE / value) {
      return Long.MAX_VALUE;
    }
    return value * multiplier;
  }

  private static long safeAdd(long left, long right) {
    if (Long.MAX_VALUE - left < right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }
}
