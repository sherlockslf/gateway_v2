package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.sideevent.SideEventOutboxDrainResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SideEventOutboxDrainWorkerTest {

  @Test
  void disabledWorkerDoesNotStartBackgroundScheduleButCanDrainManually() {
    AtomicInteger drains = new AtomicInteger();
    SideEventOutboxDrainWorker worker =
        new SideEventOutboxDrainWorker(
            () -> {
              drains.incrementAndGet();
              return CompletableFuture.completedFuture(new SideEventOutboxDrainResult(1, 1, 0));
            },
            new SideEventOutboxDrainWorkerConfig(false, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor());

    worker.start();

    assertFalse(worker.isStarted());
    assertEquals(1, worker.drainNow().toCompletableFuture().join().published());
    assertEquals(1, drains.get());
    worker.stop();
  }

  @Test
  void enabledWorkerRunsDrainOnSchedule() throws Exception {
    AtomicInteger drains = new AtomicInteger();
    SideEventOutboxDrainWorker worker =
        new SideEventOutboxDrainWorker(
            () -> {
              drains.incrementAndGet();
              return CompletableFuture.completedFuture(new SideEventOutboxDrainResult(1, 1, 0));
            },
            new SideEventOutboxDrainWorkerConfig(true, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor());

    worker.start();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (drains.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    worker.stop();

    assertTrue(drains.get() > 0);
  }

  @Test
  void workerDoesNotOverlapDrainRuns() {
    CompletableFuture<SideEventOutboxDrainResult> firstDrain = new CompletableFuture<>();
    AtomicInteger drains = new AtomicInteger();
    SideEventOutboxDrainWorker worker =
        new SideEventOutboxDrainWorker(
            () -> {
              drains.incrementAndGet();
              return firstDrain;
            },
            new SideEventOutboxDrainWorkerConfig(false, Duration.ofMillis(10)),
            Executors.newSingleThreadScheduledExecutor());

    CompletableFuture<SideEventOutboxDrainResult> runningDrain =
        worker.drainNow().toCompletableFuture();
    assertEquals(
        SideEventOutboxDrainResult.empty(), worker.drainNow().toCompletableFuture().join());

    firstDrain.complete(new SideEventOutboxDrainResult(1, 1, 0));
    assertEquals(1, runningDrain.join().published());
    assertEquals(1, drains.get());
    worker.stop();
  }
}
