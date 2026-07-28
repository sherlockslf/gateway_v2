package com.usig.infra.redis;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface RedisReplayCommands {

  CompletionStage<Void> append(
      String key, String eventId, String payload, Duration ttl, int maxEvents);

  CompletionStage<List<RedisReplayEntry>> readAfter(String key, String lastEventId);

  CompletionStage<Void> delete(String key);
}
