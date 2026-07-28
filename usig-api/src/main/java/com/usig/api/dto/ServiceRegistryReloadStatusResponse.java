package com.usig.api.dto;

import com.usig.application.port.ServiceRegistryReloadStatus;
import java.time.Instant;

public record ServiceRegistryReloadStatusResponse(
    boolean reloading,
    Instant lastAttemptedReloadAt,
    long successfulReloadCount,
    long failedReloadCount,
    Instant lastSuccessfulReloadAt,
    Integer lastSuccessfulServiceCount,
    Instant lastFailedReloadAt,
    String lastFailureMessage) {

  public static ServiceRegistryReloadStatusResponse from(ServiceRegistryReloadStatus status) {
    return new ServiceRegistryReloadStatusResponse(
        status.reloading(),
        status.lastAttemptedReloadAt(),
        status.successfulReloadCount(),
        status.failedReloadCount(),
        status.lastSuccessfulReloadAt(),
        status.lastSuccessfulServiceCount(),
        status.lastFailedReloadAt(),
        status.lastFailureMessage());
  }
}
