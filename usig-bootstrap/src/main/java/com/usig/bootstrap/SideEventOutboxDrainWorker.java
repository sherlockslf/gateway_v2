package com.usig.bootstrap;

import com.usig.application.sideevent.SideEventOutboxDrainResult;
import com.usig.application.sideevent.SideEventOutboxDrainer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class SideEventOutboxDrainWorker {

  private final Supplier<CompletionStage<SideEventOutboxDrainResult>> drainTask;
  private final SideEventOutboxDrainWorkerConfig config;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean draining = new AtomicBoolean();
  private ScheduledFuture<?> scheduledTask;

  public SideEventOutboxDrainWorker(
      SideEventOutboxDrainer drainer, SideEventOutboxDrainWorkerConfig config) {
    this(drainer::drainOnce, config, Executors.newSingleThreadScheduledExecutor());
  }

  SideEventOutboxDrainWorker(
      Supplier<CompletionStage<SideEventOutboxDrainResult>> drainTask,
      SideEventOutboxDrainWorkerConfig config,
      ScheduledExecutorService executor) {
    this.drainTask = Objects.requireNonNull(drainTask, "drainTask");
    this.config = Objects.requireNonNull(config, "config");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  public SideEventOutboxDrainWorkerConfig config() {
    return config;
  }

  public boolean isStarted() {
    return started.get();
  }

  public void start() {
    if (!config.enabled() || !started.compareAndSet(false, true)) {
      return;
    }
    long intervalMillis = Math.max(1L, config.interval().toMillis());
    scheduledTask =
        executor.scheduleWithFixedDelay(
            this::drainSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  public CompletionStage<SideEventOutboxDrainResult> drainNow() {
    return drainSafely();
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    executor.shutdownNow();
    started.set(false);
  }

  private CompletionStage<SideEventOutboxDrainResult> drainSafely() {
    if (!draining.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(SideEventOutboxDrainResult.empty());
    }
    try {
      return drainTask
          .get()
          .whenComplete(
              (ignored, failure) -> {
                draining.set(false);
              });
    } catch (RuntimeException failure) {
      draining.set(false);
      return CompletableFuture.failedFuture(failure);
    }
  }
}
