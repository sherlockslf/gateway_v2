package com.usig.application.usecase;

import com.usig.application.port.RunRepository;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class RunLifecycleEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final RunId runId;
  private final TenantId tenantId;
  private final Flow.Publisher<GatewayEvent> upstream;
  private final RunRepository runRepository;

  RunLifecycleEventPublisher(
      RunId runId,
      TenantId tenantId,
      Flow.Publisher<GatewayEvent> upstream,
      RunRepository runRepository) {
    this.runId = runId;
    this.tenantId = tenantId;
    this.upstream = upstream;
    this.runRepository = runRepository;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    upstream.subscribe(new TrackingSubscriber(subscriber));
  }

  private final class TrackingSubscriber
      implements Flow.Subscriber<GatewayEvent>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final AtomicLong demand = new AtomicLong();
    private CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
    private Flow.Subscription upstreamSubscription;

    private TrackingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      if (!subscribed.compareAndSet(false, true)) {
        subscription.cancel();
        return;
      }
      upstreamSubscription = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void request(long requested) {
      if (requested <= 0 || terminated.get()) {
        return;
      }
      addDemand(requested);
      upstreamSubscription.request(requested);
    }

    @Override
    public void cancel() {
      terminated.set(true);
      upstreamSubscription.cancel();
    }

    @Override
    public void onNext(GatewayEvent item) {
      if (terminated.get()) {
        return;
      }
      if (demand.getAndUpdate(current -> current > 0 ? current - 1 : current) <= 0) {
        onError(new IllegalStateException("downstream did not request event"));
        return;
      }
      chain = chain.thenCompose(ignored -> track(item)).thenRun(() -> downstream.onNext(item));
    }

    @Override
    public void onError(Throwable throwable) {
      if (terminated.compareAndSet(false, true)) {
        chain.whenComplete((ignored, chainError) -> downstream.onError(throwable));
      }
    }

    @Override
    public void onComplete() {
      if (terminated.compareAndSet(false, true)) {
        chain.whenComplete(
            (ignored, throwable) -> {
              if (throwable == null) {
                downstream.onComplete();
              } else {
                downstream.onError(throwable);
              }
            });
      }
    }

    private CompletionStage<Void> track(GatewayEvent event) {
      RunStatus target = targetStatus(event.eventType());
      if (target == null) {
        return CompletableFuture.completedFuture(null);
      }
      return runRepository
          .find(runId, tenantId)
          .thenCompose(existing -> transition(existing, target))
          .exceptionally(ignored -> null);
    }

    private CompletionStage<Void> transition(Optional<Run> existing, RunStatus target) {
      if (existing.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      Run run = existing.get();
      if (run.status().terminal() || run.status() == target) {
        return CompletableFuture.completedFuture(null);
      }
      if (run.status().canTransitionTo(target)) {
        return runRepository
            .compareAndSetStatus(run.id(), run.version(), target)
            .thenApply(ignored -> null);
      }
      if (run.status() == RunStatus.STARTING && target == RunStatus.WAITING_INPUT) {
        return runRepository
            .compareAndSetStatus(run.id(), run.version(), RunStatus.RUNNING)
            .thenCompose(
                updated ->
                    runRepository.compareAndSetStatus(
                        updated.id(), updated.version(), RunStatus.WAITING_INPUT))
            .thenApply(ignored -> null);
      }
      return CompletableFuture.completedFuture(null);
    }

    private void addDemand(long requested) {
      demand.getAndUpdate(
          current -> {
            long next = current + requested;
            return next < 0 ? Long.MAX_VALUE : next;
          });
    }
  }

  private static RunStatus targetStatus(EventType eventType) {
    return switch (eventType) {
      case EXECUTION_STARTED,
          EXECUTION_RUNNING,
          TEXT_START,
          TEXT_DELTA,
          TOOL_START,
          TOOL_ARGUMENTS_DELTA ->
          RunStatus.RUNNING;
      case INPUT_REQUIRED, APPROVAL_REQUIRED -> RunStatus.WAITING_INPUT;
      case EXECUTION_COMPLETED -> RunStatus.COMPLETED;
      case EXECUTION_FAILED -> RunStatus.FAILED;
      case EXECUTION_CANCELED -> RunStatus.CANCELED;
      default -> null;
    };
  }
}
