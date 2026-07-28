package com.usig.adapter.business.workflow;

import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;

public record BusinessWorkflowAdapterConfig(
    ServiceDescriptor service,
    Duration openTimeout,
    Duration firstEventTimeout,
    Duration idleEventTimeout,
    Duration totalExecutionTimeout,
    Duration cancelTimeout) {

  public BusinessWorkflowAdapterConfig(
      ServiceDescriptor service, Duration openTimeout, Duration cancelTimeout) {
    this(
        service,
        openTimeout,
        openTimeout,
        openTimeout,
        openTimeout.multipliedBy(10),
        cancelTimeout);
  }

  public BusinessWorkflowAdapterConfig {
    service = Objects.requireNonNull(service, "service must not be null");
    openTimeout = Objects.requireNonNull(openTimeout, "openTimeout must not be null");
    firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
    cancelTimeout = Objects.requireNonNull(cancelTimeout, "cancelTimeout must not be null");
    if (openTimeout.isNegative() || openTimeout.isZero()) {
      throw new IllegalArgumentException("openTimeout must be positive");
    }
    if (firstEventTimeout.isNegative() || firstEventTimeout.isZero()) {
      throw new IllegalArgumentException("firstEventTimeout must be positive");
    }
    if (idleEventTimeout.isNegative() || idleEventTimeout.isZero()) {
      throw new IllegalArgumentException("idleEventTimeout must be positive");
    }
    if (totalExecutionTimeout.isNegative() || totalExecutionTimeout.isZero()) {
      throw new IllegalArgumentException("totalExecutionTimeout must be positive");
    }
    if (cancelTimeout.isNegative() || cancelTimeout.isZero()) {
      throw new IllegalArgumentException("cancelTimeout must be positive");
    }
  }
}
