package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.port.ServiceRegistryReloadStatus;
import com.usig.application.usecase.ServiceRegistrySourceReloadResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ServiceRegistryReloadWorkerTest {

  private static final Clock FIXED_CLOCK =
      Clock.fixed(Instant.parse("2026-07-28T03:45:00Z"), ZoneOffset.UTC);

  @Test
  void disabledWorkerDoesNotStartBackgroundScheduleButCanReloadManually() {
    AtomicInteger reloads = new AtomicInteger();
    ServiceRegistryReloadWorker worker =
        new ServiceRegistryReloadWorker(
            () ->
                CompletableFuture.completedFuture(
                    new ServiceRegistrySourceReloadResult(reloads.incrementAndGet())),
            new ServiceRegistryReloadWorkerConfig(false, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor(),
            FIXED_CLOCK);

    worker.start();

    assertFalse(worker.isStarted());
    assertNull(worker.status().lastSuccessfulReloadAt());
    assertNull(worker.status().lastAttemptedReloadAt());
    assertEquals(0, worker.status().successfulReloadCount());
    assertEquals(0, worker.status().failedReloadCount());
    assertEquals(1, worker.reloadNow().toCompletableFuture().join().serviceCount());
    assertEquals(1, reloads.get());
    assertFalse(worker.status().reloading());
    assertEquals(FIXED_CLOCK.instant(), worker.status().lastAttemptedReloadAt());
    assertEquals(FIXED_CLOCK.instant(), worker.status().lastSuccessfulReloadAt());
    assertEquals(1, worker.status().lastSuccessfulServiceCount());
    assertEquals(1, worker.status().successfulReloadCount());
    assertEquals(0, worker.status().failedReloadCount());
    assertNull(worker.status().lastFailureMessage());
    worker.stop();
  }

  @Test
  void enabledWorkerRunsReloadOnSchedule() throws Exception {
    AtomicInteger reloads = new AtomicInteger();
    ServiceRegistryReloadWorker worker =
        new ServiceRegistryReloadWorker(
            () ->
                CompletableFuture.completedFuture(
                    new ServiceRegistrySourceReloadResult(reloads.incrementAndGet())),
            new ServiceRegistryReloadWorkerConfig(true, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor(),
            FIXED_CLOCK);

    worker.start();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (reloads.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    worker.stop();

    assertTrue(reloads.get() > 0);
  }

  @Test
  void workerDoesNotOverlapReloadRuns() {
    CompletableFuture<ServiceRegistrySourceReloadResult> firstReload = new CompletableFuture<>();
    AtomicInteger reloads = new AtomicInteger();
    ServiceRegistryReloadWorker worker =
        new ServiceRegistryReloadWorker(
            () -> {
              reloads.incrementAndGet();
              return firstReload;
            },
            new ServiceRegistryReloadWorkerConfig(false, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor(),
            FIXED_CLOCK);

    CompletableFuture<ServiceRegistrySourceReloadResult> runningReload =
        worker.reloadNow().toCompletableFuture();
    assertTrue(worker.status().reloading());
    assertEquals(FIXED_CLOCK.instant(), worker.status().lastAttemptedReloadAt());
    assertEquals(0, worker.reloadNow().toCompletableFuture().join().serviceCount());

    firstReload.complete(new ServiceRegistrySourceReloadResult(2));
    assertEquals(2, runningReload.join().serviceCount());
    assertEquals(1, reloads.get());
    assertFalse(worker.status().reloading());
    assertEquals(2, worker.status().lastSuccessfulServiceCount());
    assertEquals(1, worker.status().successfulReloadCount());
    assertEquals(0, worker.status().failedReloadCount());
    worker.stop();
  }

  @Test
  void workerStatusCapturesReloadFailure() {
    ServiceRegistryReloadWorker worker =
        new ServiceRegistryReloadWorker(
            () -> CompletableFuture.failedFuture(new IllegalStateException("source unavailable")),
            new ServiceRegistryReloadWorkerConfig(false, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor(),
            FIXED_CLOCK);

    assertTrue(worker.reloadNow().toCompletableFuture().isCompletedExceptionally());

    ServiceRegistryReloadStatus status = worker.status();
    assertFalse(status.reloading());
    assertEquals(FIXED_CLOCK.instant(), status.lastAttemptedReloadAt());
    assertNull(status.lastSuccessfulReloadAt());
    assertEquals(0, status.successfulReloadCount());
    assertEquals(1, status.failedReloadCount());
    assertEquals(FIXED_CLOCK.instant(), status.lastFailedReloadAt());
    assertEquals("source unavailable", status.lastFailureMessage());
    worker.stop();
  }
}
