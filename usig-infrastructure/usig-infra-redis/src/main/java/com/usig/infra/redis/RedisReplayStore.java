package com.usig.infra.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.usig.application.memory.ListGatewayEventPublisher;
import com.usig.application.port.ReplayStore;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class RedisReplayStore implements ReplayStore {

  private final RedisReplayCommands commands;
  private final ObjectMapper objectMapper;
  private final String keyPrefix;
  private final int maxEventsPerRun;

  public RedisReplayStore(RedisReplayCommands commands, String keyPrefix, int maxEventsPerRun) {
    this(commands, defaultObjectMapper(), keyPrefix, maxEventsPerRun);
  }

  RedisReplayStore(
      RedisReplayCommands commands,
      ObjectMapper objectMapper,
      String keyPrefix,
      int maxEventsPerRun) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.keyPrefix = normalizePrefix(keyPrefix);
    if (maxEventsPerRun < 1) {
      throw new IllegalArgumentException("maxEventsPerRun must be positive");
    }
    this.maxEventsPerRun = maxEventsPerRun;
  }

  @Override
  public CompletionStage<Void> append(RunId runId, GatewayEvent event, Duration ttl) {
    Objects.requireNonNull(runId, "runId must not be null");
    Objects.requireNonNull(event, "event must not be null");
    try {
      return commands.append(
          key(runId),
          event.eventId(),
          objectMapper.writeValueAsString(event),
          ttl,
          maxEventsPerRun);
    } catch (JsonProcessingException ex) {
      return CompletableFuture.failedFuture(ex);
    }
  }

  @Override
  public Flow.Publisher<GatewayEvent> readAfter(RunId runId, String lastEventId) {
    if (lastEventId == null || lastEventId.isBlank()) {
      return errorPublisher(new UseCaseException(ErrorCode.INVALID_REQUEST));
    }
    try {
      List<GatewayEvent> events =
          commands.readAfter(key(runId), lastEventId).toCompletableFuture().join().stream()
              .map(this::eventFromEntry)
              .toList();
      if (events.isEmpty()) {
        return errorPublisher(new UseCaseException(ErrorCode.CURSOR_EXPIRED));
      }
      return new ListGatewayEventPublisher(events);
    } catch (RuntimeException ex) {
      return errorPublisher(ex);
    }
  }

  @Override
  public CompletionStage<Void> delete(RunId runId) {
    return commands.delete(key(runId));
  }

  private GatewayEvent eventFromEntry(RedisReplayEntry entry) {
    try {
      return objectMapper.readValue(entry.payload(), GatewayEvent.class);
    } catch (JsonProcessingException ex) {
      throw new UseCaseException(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR);
    }
  }

  private String key(RunId runId) {
    return keyPrefix + ":run:" + runId.value() + ":events";
  }

  private static String normalizePrefix(String keyPrefix) {
    if (keyPrefix == null || keyPrefix.isBlank()) {
      return "usig";
    }
    return keyPrefix;
  }

  private static ObjectMapper defaultObjectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  private static Flow.Publisher<GatewayEvent> errorPublisher(Throwable throwable) {
    return subscriber ->
        subscriber.onSubscribe(
            new Flow.Subscription() {
              @Override
              public void request(long n) {
                subscriber.onError(throwable);
              }

              @Override
              public void cancel() {}
            });
  }
}
