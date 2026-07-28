package com.usig.domain.service;

public record ServiceLimits(
    int maxConcurrentRuns, int maxConcurrentRunsPerTenant, int maxConcurrentRunsPerGroup) {

  private static final ServiceLimits DEFAULTS = new ServiceLimits(500, 100, 500);

  public ServiceLimits(int maxConcurrentRuns, int maxConcurrentRunsPerTenant) {
    this(maxConcurrentRuns, maxConcurrentRunsPerTenant, maxConcurrentRuns);
  }

  public ServiceLimits {
    if (maxConcurrentRuns < 1) {
      throw new IllegalArgumentException("maxConcurrentRuns must be positive");
    }
    if (maxConcurrentRunsPerTenant < 1) {
      throw new IllegalArgumentException("maxConcurrentRunsPerTenant must be positive");
    }
    if (maxConcurrentRunsPerTenant > maxConcurrentRuns) {
      throw new IllegalArgumentException(
          "maxConcurrentRunsPerTenant must not exceed maxConcurrentRuns");
    }
    if (maxConcurrentRunsPerGroup < 1) {
      throw new IllegalArgumentException("maxConcurrentRunsPerGroup must be positive");
    }
  }

  public static ServiceLimits defaults() {
    return DEFAULTS;
  }
}
