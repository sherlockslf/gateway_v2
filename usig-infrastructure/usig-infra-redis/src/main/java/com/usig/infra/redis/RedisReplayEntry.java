package com.usig.infra.redis;

public record RedisReplayEntry(String eventId, String payload) {

  public RedisReplayEntry {
    if (eventId == null || eventId.isBlank()) {
      throw new IllegalArgumentException("eventId must not be blank");
    }
    if (payload == null || payload.isBlank()) {
      throw new IllegalArgumentException("payload must not be blank");
    }
  }
}
