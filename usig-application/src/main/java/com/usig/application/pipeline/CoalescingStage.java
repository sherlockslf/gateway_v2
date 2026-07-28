package com.usig.application.pipeline;

import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class CoalescingStage implements EventStage {

  private static final String NAME = "coalescing";

  private final StagePolicy policy;

  public CoalescingStage(StagePolicy policy) {
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static CoalescingStage failStream(Duration maxExecutionTime) {
    return new CoalescingStage(StagePolicy.failStream(maxExecutionTime));
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
    return subscriber -> upstream.subscribe(new CoalescingSubscriber(subscriber));
  }

  private static boolean canCoalesce(GatewayEvent left, GatewayEvent right) {
    return left.eventType() == EventType.TEXT_DELTA
        && right.eventType() == EventType.TEXT_DELTA
        && left.conversationId().equals(right.conversationId())
        && left.runId().equals(right.runId())
        && left.sourceService().equals(right.sourceService())
        && left.payload().containsKey("content")
        && right.payload().containsKey("content");
  }

  private static GatewayEvent coalesce(GatewayEvent left, GatewayEvent right) {
    Map<String, Object> payload = new HashMap<>(right.payload());
    payload.put("content", content(left) + content(right));
    return new GatewayEvent(
        right.schemaVersion(),
        right.eventId(),
        right.eventType(),
        right.conversationId(),
        right.runId(),
        right.taskId(),
        right.sourceService(),
        right.sourceEventId(),
        right.sequence(),
        right.occurredAt(),
        right.observedAt(),
        payload,
        right.metadata());
  }

  private static String content(GatewayEvent event) {
    Object content = event.payload().get("content");
    return content == null ? "" : content.toString();
  }

  private static final class CoalescingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private Flow.Subscription upstream;
    private GatewayEvent pending;
    private boolean terminated;

    private CoalescingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
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
        if (pending == null) {
          pending = event;
          return;
        }
        if (canCoalesce(pending, event)) {
          pending = coalesce(pending, event);
          return;
        }
        downstream.onNext(pending);
        pending = event;
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
        if (pending != null) {
          downstream.onNext(pending);
        }
        downstream.onComplete();
      }
    }
  }
}
