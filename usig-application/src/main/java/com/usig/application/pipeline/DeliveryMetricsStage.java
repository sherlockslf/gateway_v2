package com.usig.application.pipeline;

import com.usig.application.port.DeliveryMetric;
import com.usig.application.port.DeliveryMetricsRecorder;
import com.usig.domain.event.GatewayEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class DeliveryMetricsStage implements EventStage {

  private static final String NAME = "delivery-metrics";
  private static final String TRANSPORT_ATTRIBUTE = "transport";
  private static final String UNKNOWN_TRANSPORT = "unknown";

  private final DeliveryMetricsRecorder recorder;
  private final Clock clock;
  private final StagePolicy policy;

  public DeliveryMetricsStage(DeliveryMetricsRecorder recorder, Clock clock, StagePolicy policy) {
    this.recorder = Objects.requireNonNull(recorder, "recorder must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static DeliveryMetricsStage bestEffort(
      DeliveryMetricsRecorder recorder, Clock clock, Duration maxExecutionTime) {
    return new DeliveryMetricsStage(
        recorder, clock, new StagePolicy(maxExecutionTime, StageErrorStrategy.EMIT_WARNING));
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
    PipelineContext pipelineContext = Objects.requireNonNull(context, "context must not be null");
    return subscriber ->
        upstream.subscribe(new DeliveryMetricsSubscriber(subscriber, pipelineContext));
  }

  private DeliveryMetric metricFor(GatewayEvent event, PipelineContext context) {
    return new DeliveryMetric(
        context.serviceId(),
        event.eventType().wireName(),
        "success",
        context.attributes().getOrDefault(TRANSPORT_ATTRIBUTE, UNKNOWN_TRANSPORT));
  }

  private Duration streamAddedLatency(GatewayEvent event) {
    Duration latency = Duration.between(event.observedAt(), clock.instant());
    return latency.isNegative() ? Duration.ZERO : latency;
  }

  private final class DeliveryMetricsSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final PipelineContext context;
    private Flow.Subscription upstream;
    private boolean terminated;

    private DeliveryMetricsSubscriber(
        Flow.Subscriber<? super GatewayEvent> downstream, PipelineContext context) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
      this.context = context;
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
      GatewayEvent event = Objects.requireNonNull(item, "event must not be null");
      try {
        downstream.onNext(event);
        recordDelivery(event);
      } catch (RuntimeException exception) {
        handleFailure(exception);
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

    private void recordDelivery(GatewayEvent event) {
      DeliveryMetric metric = metricFor(event, context);
      recorder.eventOut(metric);
      recorder.streamAddedLatency(metric, streamAddedLatency(event));
    }

    private void handleFailure(RuntimeException exception) {
      if (policy.errorStrategy() == StageErrorStrategy.FAIL_STREAM) {
        terminated = true;
        upstream.cancel();
        downstream.onError(exception);
      }
    }
  }
}
