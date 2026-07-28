package com.usig.adapter.business.workflow;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class BusinessWorkflowMappedPublisher implements Flow.Publisher<GatewayEvent> {

  private final Flow.Publisher<BusinessWorkflowEvent> upstream;
  private final BusinessWorkflowEventMapper mapper;
  private final CreateRunCommand request;
  private final ExecutionContext context;
  private final String serviceId;
  private final String workflowRunId;
  private final Clock clock;
  private final Duration firstEventTimeout;
  private final Duration idleEventTimeout;
  private final Duration totalExecutionTimeout;

  BusinessWorkflowMappedPublisher(
      Flow.Publisher<BusinessWorkflowEvent> upstream,
      BusinessWorkflowEventMapper mapper,
      CreateRunCommand request,
      ExecutionContext context,
      String serviceId,
      String workflowRunId,
      Clock clock,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    this.upstream = Objects.requireNonNull(upstream, "upstream must not be null");
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId must not be null");
    this.workflowRunId = Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    this.idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    this.totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    upstream.subscribe(new MappingSubscriber(subscriber));
  }

  private final class MappingSubscriber
      implements Flow.Subscriber<BusinessWorkflowEvent>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicReference<CompletableFuture<Void>> firstEventTimer =
        new AtomicReference<>();
    private final AtomicLong idleGeneration = new AtomicLong();
    private final AtomicReference<CompletableFuture<Void>> idleTimer = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> totalExecutionTimer =
        new AtomicReference<>();
    private Flow.Subscription upstreamSubscription;
    private long sequence;
    private boolean terminated;
    private boolean firstEventSettled;

    private MappingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstreamSubscription = subscription;
      downstream.onSubscribe(this);
      scheduleFirstEventTimeout();
      scheduleTotalExecutionTimeout();
    }

    @Override
    public void onNext(BusinessWorkflowEvent item) {
      if (terminated) {
        return;
      }
      settleFirstEventWatch();
      downstream.onNext(
          mapper.map(
              item, request, context, serviceId, workflowRunId, sequence++, clock.instant()));
      scheduleIdleTimeout();
    }

    @Override
    public void onError(Throwable throwable) {
      if (!terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (!terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        downstream.onComplete();
      }
    }

    @Override
    public void request(long n) {
      if (n <= 0 || terminated) {
        return;
      }
      upstreamSubscription.request(n);
    }

    @Override
    public void cancel() {
      settleFirstEventWatch();
      settleIdleWatch();
      settleTotalExecutionWatch();
      terminated = true;
      upstreamSubscription.cancel();
    }

    private void scheduleFirstEventTimeout() {
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (!firstEventSettled && !terminated) {
                  onError(
                      new BusinessWorkflowAdapterException(
                          ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT,
                          "Business workflow adapter firstEventTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  firstEventTimeout.toMillis(), TimeUnit.MILLISECONDS));
      firstEventTimer.set(timer);
    }

    private void scheduleIdleTimeout() {
      long generation = idleGeneration.incrementAndGet();
      CompletableFuture<Void> previous = idleTimer.getAndSet(null);
      if (previous != null) {
        previous.cancel(false);
      }
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (idleGeneration.get() == generation && !terminated) {
                  onError(
                      new BusinessWorkflowAdapterException(
                          ErrorCode.DOWNSTREAM_IDLE_TIMEOUT,
                          "Business workflow adapter idleEventTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  idleEventTimeout.toMillis(), TimeUnit.MILLISECONDS));
      idleTimer.set(timer);
    }

    private void scheduleTotalExecutionTimeout() {
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (!terminated) {
                  onError(
                      new BusinessWorkflowAdapterException(
                          ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT,
                          "Business workflow adapter totalExecutionTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  totalExecutionTimeout.toMillis(), TimeUnit.MILLISECONDS));
      totalExecutionTimer.set(timer);
    }

    private void settleFirstEventWatch() {
      firstEventSettled = true;
      CompletableFuture<Void> timer = firstEventTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }

    private void settleIdleWatch() {
      idleGeneration.incrementAndGet();
      CompletableFuture<Void> timer = idleTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }

    private void settleTotalExecutionWatch() {
      CompletableFuture<Void> timer = totalExecutionTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }
  }
}
