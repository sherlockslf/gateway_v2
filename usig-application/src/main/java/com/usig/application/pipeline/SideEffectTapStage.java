package com.usig.application.pipeline;

import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventDispatcher;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Function;

public final class SideEffectTapStage implements EventStage {

  private static final String NAME = "side-effect-tap";

  private final SideEventDispatcher dispatcher;
  private final Function<GatewayEvent, Optional<SideEvent>> sideEventFactory;
  private final StagePolicy policy;

  public SideEffectTapStage(
      SideEventDispatcher dispatcher,
      Function<GatewayEvent, Optional<SideEvent>> sideEventFactory,
      StagePolicy policy) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.sideEventFactory =
        Objects.requireNonNull(sideEventFactory, "sideEventFactory must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static SideEffectTapStage bestEffort(
      SideEventDispatcher dispatcher, String topic, Duration maxExecutionTime) {
    String sideEventTopic = requireText(topic, "topic");
    return new SideEffectTapStage(
        dispatcher,
        event -> Optional.of(SideEvent.fromGatewayEvent(sideEventTopic, event, payloadFor(event))),
        new StagePolicy(maxExecutionTime, StageErrorStrategy.EMIT_WARNING));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public StageCharacteristics characteristics() {
    return new StageCharacteristics(false, false, true, true, false);
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
    return subscriber -> upstream.subscribe(new SideEffectTapSubscriber(subscriber));
  }

  private static Map<String, Object> payloadFor(GatewayEvent event) {
    return Map.of(
        "eventType", event.eventType().wireName(),
        "sourceService", event.sourceService(),
        "sequence", event.sequence(),
        "payload", event.payload());
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private final class SideEffectTapSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private Flow.Subscription upstream;
    private boolean terminated;

    private SideEffectTapSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
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
        sideEventFactory.apply(event).ifPresent(this::dispatch);
        downstream.onNext(event);
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

    private void dispatch(SideEvent sideEvent) {
      dispatcher.dispatch(sideEvent).exceptionally(ignored -> null);
    }
  }
}
