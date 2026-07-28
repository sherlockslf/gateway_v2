package com.usig.application.pipeline;

import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Flow;

public final class CriticalEventGuardStage implements EventStage {

  private static final String NAME = "critical-event-guard";
  private static final Set<EventType> NON_DISCARDABLE_EVENT_TYPES =
      Set.of(
          EventType.EXECUTION_STARTED,
          EventType.TEXT_END,
          EventType.TOOL_RESULT,
          EventType.ARTIFACT_COMPLETED,
          EventType.INPUT_REQUIRED,
          EventType.APPROVAL_REQUIRED,
          EventType.EXECUTION_FAILED,
          EventType.EXECUTION_COMPLETED,
          EventType.EXECUTION_CANCELED);

  private final StagePolicy policy;

  public CriticalEventGuardStage(StagePolicy policy) {
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static CriticalEventGuardStage failStream(Duration maxExecutionTime) {
    return new CriticalEventGuardStage(StagePolicy.failStream(maxExecutionTime));
  }

  public Set<EventType> nonDiscardableEventTypes() {
    return NON_DISCARDABLE_EVENT_TYPES;
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
    return subscriber -> upstream.subscribe(new GuardingSubscriber(subscriber));
  }

  private static boolean connectionDiagnostic(GatewayEvent event) {
    return event.eventType() == EventType.WARNING
        || event.eventType() == EventType.HEARTBEAT
        || event.eventType() == EventType.EXTENSION;
  }

  private static final class GuardingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private Flow.Subscription upstream;
    private boolean terminated;
    private boolean terminalEventSeen;

    private GuardingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
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
        if (terminalEventSeen && !connectionDiagnostic(event)) {
          throw new IllegalArgumentException(
              "GatewayEvent business event emitted after terminal event");
        }
        downstream.onNext(event);
        if (event.eventType().terminal()) {
          terminalEventSeen = true;
        }
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
