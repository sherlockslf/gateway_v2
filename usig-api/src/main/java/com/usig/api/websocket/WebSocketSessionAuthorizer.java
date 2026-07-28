package com.usig.api.websocket;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class WebSocketSessionAuthorizer {

  private final Duration sessionTtl;
  private final Clock clock;
  private final Map<String, Instant> expiresAtByConnectionId = new ConcurrentHashMap<>();

  public WebSocketSessionAuthorizer(Duration sessionTtl, Clock clock) {
    this.sessionTtl = Objects.requireNonNull(sessionTtl, "sessionTtl must not be null");
    if (sessionTtl.isZero() || sessionTtl.isNegative()) {
      throw new IllegalArgumentException("sessionTtl must be positive");
    }
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public Instant register(String connectionId) {
    String id = requireText(connectionId, "connectionId");
    Instant expiresAt = clock.instant().plus(sessionTtl);
    expiresAtByConnectionId.put(id, expiresAt);
    return expiresAt;
  }

  public boolean isAuthorized(String connectionId) {
    Instant expiresAt = expiresAtByConnectionId.get(requireText(connectionId, "connectionId"));
    return expiresAt != null && clock.instant().isBefore(expiresAt);
  }

  public Instant refresh(String connectionId) {
    return register(connectionId);
  }

  public void remove(String connectionId) {
    expiresAtByConnectionId.remove(requireText(connectionId, "connectionId"));
  }

  int sessionCount() {
    return expiresAtByConnectionId.size();
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
