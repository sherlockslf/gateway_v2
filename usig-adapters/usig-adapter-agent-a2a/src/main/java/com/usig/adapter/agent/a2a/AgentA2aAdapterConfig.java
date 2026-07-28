package com.usig.adapter.agent.a2a;

import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;

public record AgentA2aAdapterConfig(
    ServiceDescriptor service,
    int maxFrameBytes,
    Duration openTimeout,
    Duration firstEventTimeout,
    Duration idleEventTimeout,
    Duration totalExecutionTimeout,
    Duration cancelTimeout,
    String authHeaderName,
    String bearerToken) {

  public AgentA2aAdapterConfig {
    service = Objects.requireNonNull(service, "service must not be null");
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
    authHeaderName = normalize(authHeaderName);
    bearerToken = normalize(bearerToken);
  }

  public AgentA2aAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      Duration cancelTimeout,
      String authHeaderName,
      String bearerToken) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        openTimeout,
        cancelTimeout,
        authHeaderName,
        bearerToken);
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
