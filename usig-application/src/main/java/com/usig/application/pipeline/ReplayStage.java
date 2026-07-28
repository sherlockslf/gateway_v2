package com.usig.application.pipeline;

import com.usig.application.port.ReplayStore;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class ReplayStage implements EventStage {

  private static final String NAME = "replay";

  private final ReplayStore replayStore;
  private final Duration ttl;
  private final StagePolicy policy;

  public ReplayStage(ReplayStore replayStore, Duration ttl, StagePolicy policy) {
    this.replayStore = Objects.requireNonNull(replayStore, "replayStore must not be null");
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
    this.ttl = ttl;
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static ReplayStage failStream(
      ReplayStore replayStore, Duration ttl, Duration maxExecutionTime) {
    return new ReplayStage(replayStore, ttl, StagePolicy.failStream(maxExecutionTime));
  }

  public ReplayStore replayStore() {
    return replayStore;
  }

  public Duration ttl() {
    return ttl;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public StageCharacteristics characteristics() {
    return new StageCharacteristics(true, true, true, false, true);
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
    return subscriber -> upstream.subscribe(new ReplaySubscriber(subscriber, context.runId()));
  }

  private final class ReplaySubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final RunId runId;
    private Flow.Subscription upstream;
    private boolean terminated;

    private ReplaySubscriber(Flow.Subscriber<? super GatewayEvent> downstream, RunId runId) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
      this.runId = runId;
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
        replayStore.append(runId, event, ttl).toCompletableFuture().join();
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
  }
}
