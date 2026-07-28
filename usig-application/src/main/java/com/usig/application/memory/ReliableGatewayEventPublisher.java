package com.usig.application.memory;

import com.usig.application.port.ReplayStore;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

final class ReliableGatewayEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final RunId runId;
  private final Flow.Publisher<GatewayEvent> source;
  private final ReplayStore replayStore;
  private final ReliabilityOptions options;

  ReliableGatewayEventPublisher(
      RunId runId,
      Flow.Publisher<GatewayEvent> source,
      ReplayStore replayStore,
      ReliabilityOptions options) {
    this.runId = Objects.requireNonNull(runId, "runId must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.replayStore = Objects.requireNonNull(replayStore, "replayStore must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    source.subscribe(new ReliableSubscriber(subscriber));
  }

  private final class ReliableSubscriber
      implements Flow.Subscriber<GatewayEvent>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicLong requested = new AtomicLong();
    private final Queue<GatewayEvent> pending = new ArrayDeque<>();
    private final RollingTextRedactor redactor =
        new RollingTextRedactor(options.redactionLiterals());
    private Flow.Subscription upstream;
    private int pendingBytes;
    private boolean terminated;
    private boolean upstreamCompleted;

    private ReliableSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstream = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void onNext(GatewayEvent item) {
      if (terminated) {
        return;
      }
      try {
        enqueue(item);
        drain();
        requestMoreIfNeeded();
      } catch (RuntimeException ex) {
        onError(ex);
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
      if (terminated) {
        return;
      }
      upstreamCompleted = true;
      drain();
    }

    @Override
    public void request(long n) {
      if (n <= 0 || terminated) {
        return;
      }
      requested.addAndGet(n);
      drain();
      requestMoreIfNeeded();
    }

    @Override
    public void cancel() {
      terminated = true;
      upstream.cancel();
    }

    private void enqueue(GatewayEvent event) {
      GatewayEvent eventToAdd = event;
      if (options.coalesceTextDelta() && !pending.isEmpty()) {
        GatewayEvent last = ((ArrayDeque<GatewayEvent>) pending).peekLast();
        if (GatewayEventTransforms.canCoalesce(last, event)) {
          pendingBytes -= GatewayEventTransforms.estimateBytes(last);
          ((ArrayDeque<GatewayEvent>) pending).removeLast();
          eventToAdd = GatewayEventTransforms.coalesce(last, event);
        }
      }
      eventToAdd = GatewayEventTransforms.redact(eventToAdd, redactor);
      pending.add(eventToAdd);
      pendingBytes += GatewayEventTransforms.estimateBytes(eventToAdd);
      if (pending.size() > options.maxPendingEvents() || pendingBytes > options.maxPendingBytes()) {
        throw new UseCaseException(ErrorCode.CLIENT_TOO_SLOW);
      }
    }

    private void drain() {
      while (!pending.isEmpty() && requested.get() > 0 && !terminated) {
        if (shouldHoldBackSingleTextDelta()) {
          break;
        }
        GatewayEvent event = pending.remove();
        pendingBytes -= GatewayEventTransforms.estimateBytes(event);
        replayStore.append(runId, event, options.replayTtl()).toCompletableFuture().join();
        downstream.onNext(event);
        requested.decrementAndGet();
      }
      if (upstreamCompleted && pending.isEmpty() && !terminated) {
        terminated = true;
        downstream.onComplete();
      }
    }

    private void requestMoreIfNeeded() {
      if (!terminated
          && !upstreamCompleted
          && (pending.isEmpty() || shouldHoldBackSingleTextDelta())
          && requested.get() > 0) {
        upstream.request(1);
      }
    }

    private boolean shouldHoldBackSingleTextDelta() {
      return !upstreamCompleted
          && pending.size() == 1
          && pending.peek().eventType() == EventType.TEXT_DELTA;
    }
  }
}
