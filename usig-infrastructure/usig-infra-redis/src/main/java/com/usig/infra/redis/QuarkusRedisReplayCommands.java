package com.usig.infra.redis;

import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Request;
import io.vertx.mutiny.redis.client.Response;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public final class QuarkusRedisReplayCommands implements RedisReplayCommands {

  private final Redis redis;

  @Inject
  public QuarkusRedisReplayCommands(Redis redis) {
    this.redis = redis;
  }

  @Override
  public CompletionStage<Void> append(
      String key, String eventId, String payload, Duration ttl, int maxEvents) {
    Request xadd =
        Request.cmd(Command.XADD)
            .arg(key)
            .arg("MAXLEN")
            .arg("~")
            .arg(maxEvents)
            .arg(eventId)
            .arg("eventId")
            .arg(eventId)
            .arg("payload")
            .arg(payload);
    Request expire = Request.cmd(Command.EXPIRE).arg(key).arg(ttl.toSeconds());
    return redis
        .send(xadd)
        .chain(ignored -> redis.send(expire))
        .replaceWithVoid()
        .subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<List<RedisReplayEntry>> readAfter(String key, String lastEventId) {
    Request request =
        Request.cmd(Command.XRANGE).arg(key).arg("(" + lastEventId).arg("+").arg("COUNT").arg(256);
    return redis.send(request).map(this::entries).subscribeAsCompletionStage();
  }

  @Override
  public CompletionStage<Void> delete(String key) {
    return redis
        .send(Request.cmd(Command.DEL).arg(key))
        .replaceWithVoid()
        .subscribeAsCompletionStage();
  }

  private List<RedisReplayEntry> entries(Response response) {
    List<RedisReplayEntry> entries = new ArrayList<>();
    for (Response streamEntry : response) {
      Response fields = streamEntry.get(1);
      String eventId = null;
      String payload = null;
      for (int i = 0; i < fields.size(); i += 2) {
        String name = fields.get(i).toString();
        String value = fields.get(i + 1).toString();
        if ("eventId".equals(name)) {
          eventId = value;
        } else if ("payload".equals(name)) {
          payload = value;
        }
      }
      if (eventId != null && payload != null) {
        entries.add(new RedisReplayEntry(eventId, payload));
      }
    }
    return entries;
  }
}
