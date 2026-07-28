package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class TimestampNormalizationStage implements EventStage {

  private static final String NAME = "timestamp-normalization";

  private final StagePolicy policy;

  public TimestampNormalizationStage(StagePolicy policy) {
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static TimestampNormalizationStage failStream(Duration maxExecutionTime) {
    return new TimestampNormalizationStage(StagePolicy.failStream(maxExecutionTime));
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
    return subscriber -> upstream.subscribe(new NormalizingSubscriber(subscriber));
  }

  private static GatewayEvent normalize(GatewayEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    Instant observedAt = event.observedAt();
    Instant occurredAt = event.occurredAt();
    if (!occurredAt.isAfter(observedAt)) {
      return event;
    }
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
        observedAt,
        observedAt,
        event.payload(),
        event.metadata());
  }

  private static final class NormalizingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private Flow.Subscription upstream;
    private boolean terminated;

    private NormalizingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
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
        downstream.onNext(normalize(item));
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
