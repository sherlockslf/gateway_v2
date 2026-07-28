package com.usig.application.pipeline;

import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class RollingRedactionStage implements EventStage {

  private static final String NAME = "rolling-redaction";
  private static final String CONTENT_FIELD = "content";
  private static final String REDACTED = "[REDACTED]";

  private final List<String> redactionLiterals;
  private final StagePolicy policy;

  public RollingRedactionStage(List<String> redactionLiterals, StagePolicy policy) {
    this.redactionLiterals = List.copyOf(Objects.requireNonNull(redactionLiterals));
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public static RollingRedactionStage failStream(
      List<String> redactionLiterals, Duration maxExecutionTime) {
    return new RollingRedactionStage(redactionLiterals, StagePolicy.failStream(maxExecutionTime));
  }

  public List<String> redactionLiterals() {
    return redactionLiterals;
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
    return subscriber -> upstream.subscribe(new RedactionSubscriber(subscriber, redactionLiterals));
  }

  private static GatewayEvent redact(GatewayEvent event, List<String> redactionLiterals) {
    Objects.requireNonNull(event, "event must not be null");
    if (event.eventType() != EventType.TEXT_DELTA || !event.payload().containsKey(CONTENT_FIELD)) {
      return event;
    }
    Map<String, Object> payload = new java.util.HashMap<>(event.payload());
    payload.put(
        CONTENT_FIELD, redactContent(event.payload().get(CONTENT_FIELD), redactionLiterals));
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

  private static String redactContent(Object content, List<String> redactionLiterals) {
    String redacted = content == null ? "" : content.toString();
    for (String literal : redactionLiterals) {
      if (literal != null && !literal.isBlank()) {
        redacted = redacted.replace(literal, REDACTED);
      }
    }
    return redacted;
  }

  private static final class RedactionSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final List<String> redactionLiterals;
    private Flow.Subscription upstream;
    private boolean terminated;

    private RedactionSubscriber(
        Flow.Subscriber<? super GatewayEvent> downstream, List<String> redactionLiterals) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
      this.redactionLiterals = redactionLiterals;
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
        downstream.onNext(redact(item, redactionLiterals));
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
