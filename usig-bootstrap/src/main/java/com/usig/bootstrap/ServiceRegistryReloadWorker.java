package com.usig.bootstrap;

import com.usig.application.port.ServiceRegistryReloadStatus;
import com.usig.application.port.ServiceRegistryReloadStatusReader;
import com.usig.application.usecase.ReloadServiceRegistryFromSourceUseCase;
import com.usig.application.usecase.ServiceRegistrySourceReloadResult;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class ServiceRegistryReloadWorker implements ServiceRegistryReloadStatusReader {

  private final Supplier<CompletionStage<ServiceRegistrySourceReloadResult>> reloadTask;
  private final ServiceRegistryReloadWorkerConfig config;
  private final ScheduledExecutorService executor;
  private final Clock clock;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean reloading = new AtomicBoolean();
  private final AtomicReference<ServiceRegistryReloadStatus> status =
      new AtomicReference<>(ServiceRegistryReloadStatus.initial());
  private ScheduledFuture<?> scheduledTask;

  public ServiceRegistryReloadWorker(
      ReloadServiceRegistryFromSourceUseCase reloadUseCase,
      ServiceRegistryReloadWorkerConfig config) {
    this(
        reloadUseCase::reload,
        config,
        Executors.newSingleThreadScheduledExecutor(),
        Clock.systemUTC());
  }

  ServiceRegistryReloadWorker(
      Supplier<CompletionStage<ServiceRegistrySourceReloadResult>> reloadTask,
      ServiceRegistryReloadWorkerConfig config,
      ScheduledExecutorService executor) {
    this(reloadTask, config, executor, Clock.systemUTC());
  }

  ServiceRegistryReloadWorker(
      Supplier<CompletionStage<ServiceRegistrySourceReloadResult>> reloadTask,
      ServiceRegistryReloadWorkerConfig config,
      ScheduledExecutorService executor,
      Clock clock) {
    this.reloadTask = Objects.requireNonNull(reloadTask, "reloadTask");
    this.config = Objects.requireNonNull(config, "config");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public ServiceRegistryReloadWorkerConfig config() {
    return config;
  }

  public boolean isStarted() {
    return started.get();
  }

  @Override
  public ServiceRegistryReloadStatus status() {
    return status.get().withReloading(reloading.get());
  }

  public void start() {
    if (!config.enabled() || !started.compareAndSet(false, true)) {
      return;
    }
    long intervalMillis = Math.max(1L, config.interval().toMillis());
    scheduledTask =
        executor.scheduleWithFixedDelay(
            this::reloadSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  public CompletionStage<ServiceRegistrySourceReloadResult> reloadNow() {
    return reloadSafely();
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    executor.shutdownNow();
    started.set(false);
  }

  private CompletionStage<ServiceRegistrySourceReloadResult> reloadSafely() {
    if (!reloading.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(new ServiceRegistrySourceReloadResult(0));
    }
    status.updateAndGet(previous -> previous.attempted(clock.instant()));
    try {
      return reloadTask
          .get()
          .whenComplete(
              (result, failure) -> {
                if (failure == null) {
                  status.updateAndGet(
                      previous -> previous.successful(clock.instant(), result.serviceCount()));
                } else {
                  status.updateAndGet(previous -> previous.failed(clock.instant(), failure));
                }
                reloading.set(false);
              });
    } catch (RuntimeException failure) {
      status.updateAndGet(previous -> previous.failed(clock.instant(), failure));
      reloading.set(false);
      return CompletableFuture.failedFuture(failure);
    }
  }
}
