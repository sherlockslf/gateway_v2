package com.usig.adapter.generic.sse;

import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;

public record GenericSseAdapterConfig(
    ServiceDescriptor service,
    GenericSseMappingProfile mappingProfile,
    int maxFrameBytes,
    Duration openTimeout,
    Duration firstEventTimeout,
    Duration idleEventTimeout,
    Duration totalExecutionTimeout,
    Duration cancelTimeout) {

  private static final Duration DEFAULT_TOTAL_EXECUTION_TIMEOUT = Duration.ofHours(24);
  private static final Duration DEFAULT_CANCEL_TIMEOUT = Duration.ofSeconds(30);

  public GenericSseAdapterConfig {
    service = Objects.requireNonNull(service, "service must not be null");
    mappingProfile = Objects.requireNonNull(mappingProfile, "mappingProfile must not be null");
    if (maxFrameBytes < 1) {
      throw new IllegalArgumentException("maxFrameBytes must be positive");
    }
    openTimeout = Objects.requireNonNull(openTimeout, "openTimeout must not be null");
    if (openTimeout.isZero() || openTimeout.isNegative()) {
      throw new IllegalArgumentException("openTimeout must be positive");
    }
    firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    if (firstEventTimeout.isZero() || firstEventTimeout.isNegative()) {
      throw new IllegalArgumentException("firstEventTimeout must be positive");
    }
    idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    if (idleEventTimeout.isZero() || idleEventTimeout.isNegative()) {
      throw new IllegalArgumentException("idleEventTimeout must be positive");
    }
    totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
    if (totalExecutionTimeout.isZero() || totalExecutionTimeout.isNegative()) {
      throw new IllegalArgumentException("totalExecutionTimeout must be positive");
    }
    cancelTimeout = Objects.requireNonNull(cancelTimeout, "cancelTimeout must not be null");
    if (cancelTimeout.isZero() || cancelTimeout.isNegative()) {
      throw new IllegalArgumentException("cancelTimeout must be positive");
    }
  }

  public GenericSseAdapterConfig(
      ServiceDescriptor service,
      GenericSseMappingProfile mappingProfile,
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    this(
        service,
        mappingProfile,
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        DEFAULT_CANCEL_TIMEOUT);
  }

  public GenericSseAdapterConfig(
      ServiceDescriptor service,
      GenericSseMappingProfile mappingProfile,
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout) {
    this(
        service,
        mappingProfile,
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT);
  }

  public GenericSseAdapterConfig(
      ServiceDescriptor service,
      GenericSseMappingProfile mappingProfile,
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout) {
    this(
        service,
        mappingProfile,
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        firstEventTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT);
  }

  public GenericSseAdapterConfig(
      ServiceDescriptor service,
      GenericSseMappingProfile mappingProfile,
      int maxFrameBytes,
      Duration openTimeout) {
    this(
        service,
        mappingProfile,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT);
  }
}
