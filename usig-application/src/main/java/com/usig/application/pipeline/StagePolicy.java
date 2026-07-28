package com.usig.application.pipeline;

import java.time.Duration;

public record StagePolicy(Duration maxExecutionTime, StageErrorStrategy errorStrategy) {

  public StagePolicy {
    if (maxExecutionTime == null || maxExecutionTime.isZero() || maxExecutionTime.isNegative()) {
      throw new IllegalArgumentException("maxExecutionTime must be positive");
    }
    if (errorStrategy == null) {
      throw new IllegalArgumentException("errorStrategy must not be null");
    }
  }

  public static StagePolicy failStream(Duration maxExecutionTime) {
    return new StagePolicy(maxExecutionTime, StageErrorStrategy.FAIL_STREAM);
  }
}
