package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public final class SequenceStage implements EventStage {

  private static final String NAME = "sequence";

  private final long initialSequence;
  private final StagePolicy policy;

  public SequenceStage(long initialSequence, StagePolicy policy) {
    if (initialSequence < 0) {
      throw new IllegalArgumentException("initialSequence must not be negative");
    }
    this.initialSequence = initialSequence;
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static SequenceStage failStream(long initialSequence, Duration maxExecutionTime) {
    return new SequenceStage(initialSequence, StagePolicy.failStream(maxExecutionTime));
  }

  public long initialSequence() {
    return initialSequence;
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
    AtomicLong nextSequence = new AtomicLong(initialSequence);
    return subscriber -> upstream.subscribe(new SequencingSubscriber(subscriber, nextSequence));
  }

  private static GatewayEvent withSequence(GatewayEvent event, long sequence) {
    Objects.requireNonNull(event, "event must not be null");
    return new GatewayEvent(
        event.schemaVersion(),
        event.eventId(),
        event.eventType(),
        event.conversationId(),
        event.runId(),
        event.taskId(),
        event.sourceService(),
        event.sourceEventId(),
        sequence,
        event.occurredAt(),
        event.observedAt(),
        event.payload(),
        event.metadata());
  }

  private static final class SequencingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicLong nextSequence;
    private Flow.Subscription upstream;
    private boolean terminated;

    private SequencingSubscriber(
        Flow.Subscriber<? super GatewayEvent> downstream, AtomicLong nextSequence) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
      this.nextSequence = nextSequence;
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
        downstream.onNext(withSequence(item, nextSequence.getAndIncrement()));
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
