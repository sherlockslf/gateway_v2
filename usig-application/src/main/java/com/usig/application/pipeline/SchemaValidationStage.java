package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class SchemaValidationStage implements EventStage {

  private static final String NAME = "schema-validation";

  private final StagePolicy policy;

  public SchemaValidationStage(StagePolicy policy) {
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static SchemaValidationStage failStream(Duration maxExecutionTime) {
    return new SchemaValidationStage(StagePolicy.failStream(maxExecutionTime));
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
    return subscriber -> upstream.subscribe(new ValidatingSubscriber(subscriber, context));
  }

  private static void validate(GatewayEvent event, PipelineContext context) {
    Objects.requireNonNull(event, "event must not be null");
    if (!GatewayEvent.CURRENT_SCHEMA_VERSION.equals(event.schemaVersion())) {
      throw new IllegalArgumentException(
          "Unsupported GatewayEvent schemaVersion: " + event.schemaVersion());
    }
    if (!context.runId().value().equals(event.runId())) {
      throw new IllegalArgumentException("GatewayEvent runId does not match pipeline context");
    }
    if (!context.tenantId().value().equals(event.metadata().tenantId())) {
      throw new IllegalArgumentException("GatewayEvent tenantId does not match pipeline context");
    }
    if (!context.serviceId().equals(event.sourceService())) {
      throw new IllegalArgumentException(
          "GatewayEvent sourceService does not match pipeline context");
    }
  }

  private static final class ValidatingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final PipelineContext context;
    private Flow.Subscription upstream;
    private boolean terminated;

    private ValidatingSubscriber(
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
      try {
        validate(item, context);
        downstream.onNext(item);
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
