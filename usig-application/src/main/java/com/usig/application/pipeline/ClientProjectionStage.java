package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.function.Function;

public final class ClientProjectionStage implements EventStage {

  private static final String NAME = "client-projection";

  private final Function<GatewayEvent, Optional<GatewayEvent>> projection;
  private final StagePolicy policy;

  public ClientProjectionStage(
      Function<GatewayEvent, Optional<GatewayEvent>> projection, StagePolicy policy) {
    this.projection = Objects.requireNonNull(projection, "projection must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static ClientProjectionStage hidePayloadKeys(
      Set<String> hiddenPayloadKeys, Duration maxExecutionTime) {
    Set<String> hiddenKeys =
        Set.copyOf(Objects.requireNonNull(hiddenPayloadKeys, "hiddenPayloadKeys must not be null"));
    return new ClientProjectionStage(
        event -> Optional.of(copyWithPayload(event, publicPayload(event.payload(), hiddenKeys))),
        StagePolicy.failStream(maxExecutionTime));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public StageCharacteristics characteristics() {
    return StageCharacteristics.synchronousNonBlockingOrdered();
  }

  @Override
  public StagePolicy policy() {
    return policy;
  }

  @Override
  public Flow.Publisher<GatewayEvent> apply(
      Flow.Publisher<GatewayEvent> upstream, PipelineContext context) {
    Objects.requireNonNull(upstream, "upstream must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return subscriber -> upstream.subscribe(new ClientProjectionSubscriber(subscriber));
  }

  private static Map<String, Object> publicPayload(
      Map<String, Object> payload, Set<String> hiddenPayloadKeys) {
    if (payload.isEmpty() || hiddenPayloadKeys.isEmpty()) {
      return payload;
    }
    Map<String, Object> projected = new LinkedHashMap<>();
    payload.forEach(
        (key, value) -> {
          if (!hiddenPayloadKeys.contains(key)) {
            projected.put(key, value);
          }
        });
    return Map.copyOf(projected);
  }

  private static GatewayEvent copyWithPayload(GatewayEvent event, Map<String, Object> payload) {
    return new GatewayEvent(
        event.schemaVersion(),
        event.eventId(),
        event.eventType(),
        event.conversationId(),
        event.runId(),
        event.taskId(),
        event.sourceService(),
        event.sourceEventId(),
        event.sequence(),
        event.occurredAt(),
        event.observedAt(),
        payload,
        event.metadata());
  }

  private final class ClientProjectionSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private Flow.Subscription upstream;
    private boolean terminated;

    private ClientProjectionSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstream = Objects.requireNonNull(subscription, "subscription must not be null");
      downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(GatewayEvent item) {
      if (terminated) {
        return;
      }
      try {
        GatewayEvent event = Objects.requireNonNull(item, "event must not be null");
        projection.apply(event).ifPresent(downstream::onNext);
      } catch (RuntimeException exception) {
        terminated = true;
        upstream.cancel();
        downstream.onError(exception);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (!terminated) {
        terminated = true;
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (!terminated) {
        terminated = true;
        downstream.onComplete();
      }
    }
  }
}
