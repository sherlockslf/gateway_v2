package com.usig.application.sideevent;

import com.usig.application.port.SideEventOutbox;
import com.usig.application.port.SideEventPublisher;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class SideEventDispatcher {

  private final SideEventPublisher publisher;
  private final SideEventDispatchPolicy policy;
  private final Optional<SideEventOutbox> outbox;
  private final Clock clock;

  public SideEventDispatcher(SideEventPublisher publisher, SideEventDispatchPolicy policy) {
    this(publisher, policy, null, Clock.systemUTC());
  }

  public SideEventDispatcher(
      SideEventPublisher publisher,
      SideEventDispatchPolicy policy,
      SideEventOutbox outbox,
      Clock clock) {
    this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.outbox = Optional.ofNullable(outbox);
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public CompletionStage<Void> dispatch(SideEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    boolean failFast = policy.shouldFailFast(event);
    CompletionStage<Void> published;
    try {
      published = publisher.publish(event);
    } catch (RuntimeException exception) {
      return handleFailure(event, exception, failFast);
    }
    if (failFast) {
      return published
          .handle((ignored, failure) -> failure == null ? null : failAfterOutbox(event, failure))
          .thenCompose(result -> result == null ? CompletableFuture.completedFuture(null) : result);
    }
    return published
        .handle((ignored, failure) -> failure == null ? null : recordOutbox(event, failure))
        .thenCompose(result -> result == null ? CompletableFuture.completedFuture(null) : result);
  }

  private CompletionStage<Void> handleFailure(
      SideEvent event, RuntimeException failure, boolean failFast) {
    if (failFast) {
      return failAfterOutbox(event, failure);
    }
    return recordOutbox(event, failure);
  }

  private CompletionStage<Void> recordOutbox(SideEvent event, Throwable failure) {
    return outbox
        .map(sideEventOutbox -> sideEventOutbox.recordFailure(event, failure, clock.instant()))
        .orElseGet(() -> CompletableFuture.completedFuture(null))
        .exceptionally(ignored -> null);
  }

  private CompletionStage<Void> failAfterOutbox(SideEvent event, Throwable failure) {
    return recordOutbox(event, failure)
        .thenCompose(
            ignored ->
                CompletableFuture.failedFuture(
                    failure instanceof RuntimeException runtimeFailure
                        ? runtimeFailure
                        : new SideEventDispatchException("Side event publish failed", failure)));
  }
}
