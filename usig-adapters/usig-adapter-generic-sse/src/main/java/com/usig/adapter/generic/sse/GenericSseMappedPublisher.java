package com.usig.adapter.generic.sse;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class GenericSseMappedPublisher implements Flow.Publisher<GatewayEvent> {

  private static final int MAX_PENDING_FRAMES = 256;

  private final Flow.Publisher<byte[]> source;
  private final SseFrameDecoder decoder;
  private final GenericSseEventMapper mapper;
  private final CreateRunCommand request;
  private final ExecutionContext context;
  private final String serviceId;
  private final String downstreamTaskId;
  private final Duration firstEventTimeout;
  private final Duration idleEventTimeout;
  private final Duration totalExecutionTimeout;

  GenericSseMappedPublisher(
      Flow.Publisher<byte[]> source,
      SseFrameDecoder decoder,
      GenericSseEventMapper mapper,
      CreateRunCommand request,
      ExecutionContext context,
      String serviceId,
      String downstreamTaskId,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.decoder = Objects.requireNonNull(decoder, "decoder must not be null");
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId must not be null");
    this.downstreamTaskId = downstreamTaskId;
    this.firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    this.idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    this.totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    source.subscribe(new MappingSubscriber(subscriber));
  }

  private final class MappingSubscriber implements Flow.Subscriber<byte[]>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicLong requested = new AtomicLong();
    private final AtomicReference<CompletableFuture<Void>> firstEventTimer =
        new AtomicReference<>();
    private final AtomicLong idleGeneration = new AtomicLong();
    private final AtomicReference<CompletableFuture<Void>> idleTimer = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> totalExecutionTimer =
        new AtomicReference<>();
    private final Queue<SseFrame> pendingFrames = new ArrayDeque<>();
    private Flow.Subscription upstream;
    private long sequence;
    private boolean terminated;
    private boolean upstreamCompleted;
    private boolean firstEventSettled;

    private MappingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstream = subscription;
      downstream.onSubscribe(this);
      scheduleFirstEventTimeout();
      scheduleTotalExecutionTimeout();
    }

    @Override
    public void onNext(byte[] item) {
      if (terminated) {
        return;
      }
      try {
        enqueue(decoder.append(item));
        drain();
        requestMoreIfNeeded();
      } catch (RuntimeException ex) {
        onError(ex);
      }
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
      if (terminated) {
        return;
      }
      try {
        enqueue(decoder.finish());
        upstreamCompleted = true;
        drain();
      } catch (RuntimeException ex) {
        onError(ex);
      }
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
      settleFirstEventWatch();
      settleIdleWatch();
      settleTotalExecutionWatch();
      terminated = true;
      upstream.cancel();
    }

    private void enqueue(List<SseFrame> frames) {
      if (pendingFrames.size() + frames.size() > MAX_PENDING_FRAMES) {
        throw new GenericSseAdapterException("Mapped SSE pending frame buffer exceeded");
      }
      pendingFrames.addAll(frames);
    }

    private void drain() {
      while (!pendingFrames.isEmpty() && requested.get() > 0 && !terminated) {
        SseFrame frame = pendingFrames.remove();
        GatewayEvent event =
            mapper.map(
                frame, request, context, serviceId, downstreamTaskId, sequence, Instant.now());
        settleFirstEventWatch();
        downstream.onNext(event);
        scheduleIdleTimeout();
        sequence++;
        requested.decrementAndGet();
      }
      if (upstreamCompleted && pendingFrames.isEmpty() && !terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        downstream.onComplete();
      }
    }

    private void requestMoreIfNeeded() {
      if (!terminated && !upstreamCompleted && pendingFrames.isEmpty() && requested.get() > 0) {
        upstream.request(1);
      }
    }

    private void scheduleFirstEventTimeout() {
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (!firstEventSettled && !terminated) {
                  onError(
                      new GenericSseAdapterException(
                          ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT,
                          "Generic SSE adapter firstEventTimeout elapsed"));
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
                      new GenericSseAdapterException(
                          ErrorCode.DOWNSTREAM_IDLE_TIMEOUT,
                          "Generic SSE adapter idleEventTimeout elapsed"));
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
                      new GenericSseAdapterException(
                          ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT,
                          "Generic SSE adapter totalExecutionTimeout elapsed"));
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
