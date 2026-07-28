package com.usig.api.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WebSocketSessionAuthorizerTest {

  private final AtomicReference<Instant> now =
      new AtomicReference<>(Instant.parse("2026-07-28T03:20:00Z"));
  private final Clock clock =
      new Clock() {
        @Override
        public ZoneOffset getZone() {
          return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
          return this;
        }

        @Override
        public Instant instant() {
          return now.get();
        }
      };

  @Test
  void authorizesUntilConfiguredTtlExpires() {
    WebSocketSessionAuthorizer authorizer =
        new WebSocketSessionAuthorizer(Duration.ofSeconds(5), clock);

    authorizer.register("connection-1");
    now.set(Instant.parse("2026-07-28T03:20:04Z"));
    assertTrue(authorizer.isAuthorized("connection-1"));

    now.set(Instant.parse("2026-07-28T03:20:05Z"));
    assertFalse(authorizer.isAuthorized("connection-1"));
  }

  @Test
  void refreshRenewsSessionExpiry() {
    WebSocketSessionAuthorizer authorizer =
        new WebSocketSessionAuthorizer(Duration.ofSeconds(5), clock);

    authorizer.register("connection-1");
    now.set(Instant.parse("2026-07-28T03:20:04Z"));
    authorizer.refresh("connection-1");
    now.set(Instant.parse("2026-07-28T03:20:08Z"));

    assertTrue(authorizer.isAuthorized("connection-1"));
  }

  @Test
  void removeRevokesSessionState() {
    WebSocketSessionAuthorizer authorizer =
        new WebSocketSessionAuthorizer(Duration.ofSeconds(5), clock);

    authorizer.register("connection-1");
    assertEquals(1, authorizer.sessionCount());
    authorizer.remove("connection-1");

    assertFalse(authorizer.isAuthorized("connection-1"));
    assertEquals(0, authorizer.sessionCount());
  }
}
