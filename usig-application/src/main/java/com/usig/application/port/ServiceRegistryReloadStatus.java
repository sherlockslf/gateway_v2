package com.usig.application.port;

import java.time.Instant;

public record ServiceRegistryReloadStatus(
    boolean reloading,
    Instant lastAttemptedReloadAt,
    long successfulReloadCount,
    long failedReloadCount,
    Instant lastSuccessfulReloadAt,
    Integer lastSuccessfulServiceCount,
    Instant lastFailedReloadAt,
    String lastFailureMessage) {

  public static ServiceRegistryReloadStatus initial() {
    return new ServiceRegistryReloadStatus(false, null, 0, 0, null, null, null, null);
  }

  public ServiceRegistryReloadStatus withReloading(boolean reloading) {
    return new ServiceRegistryReloadStatus(
        reloading,
        lastAttemptedReloadAt,
        successfulReloadCount,
        failedReloadCount,
        lastSuccessfulReloadAt,
        lastSuccessfulServiceCount,
        lastFailedReloadAt,
        lastFailureMessage);
  }

  public ServiceRegistryReloadStatus attempted(Instant at) {
    return new ServiceRegistryReloadStatus(
        true,
        at,
        successfulReloadCount,
        failedReloadCount,
        lastSuccessfulReloadAt,
        lastSuccessfulServiceCount,
        lastFailedReloadAt,
        lastFailureMessage);
  }

  public ServiceRegistryReloadStatus successful(Instant at, int serviceCount) {
    return new ServiceRegistryReloadStatus(
        false,
        lastAttemptedReloadAt,
        successfulReloadCount + 1,
        failedReloadCount,
        at,
        serviceCount,
        null,
        null);
  }

  public ServiceRegistryReloadStatus failed(Instant at, Throwable failure) {
    return new ServiceRegistryReloadStatus(
        false,
        lastAttemptedReloadAt,
        successfulReloadCount,
        failedReloadCount + 1,
        lastSuccessfulReloadAt,
        lastSuccessfulServiceCount,
        at,
        failure == null ? null : failure.getMessage());
  }
}
