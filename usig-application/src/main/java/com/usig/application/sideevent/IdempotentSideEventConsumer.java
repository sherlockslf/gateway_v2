package com.usig.application.sideevent;

import com.usig.application.port.SideEventConsumptionStore;
import com.usig.application.port.SideEventHandler;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class IdempotentSideEventConsumer {

  private final SideEventConsumptionStore store;
  private final SideEventHandler handler;

  public IdempotentSideEventConsumer(SideEventConsumptionStore store, SideEventHandler handler) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
  }

  public CompletionStage<SideEventConsumptionResult> consume(SideEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    return store.claim(event.eventId()).thenCompose(claim -> consumeClaimed(event, claim));
  }

  private CompletionStage<SideEventConsumptionResult> consumeClaimed(
      SideEvent event, SideEventConsumptionClaim claim) {
    if (claim == SideEventConsumptionClaim.CONSUMED) {
      return CompletableFuture.completedFuture(
          SideEventConsumptionResult.skippedConsumed(event.eventId()));
    }
    if (claim == SideEventConsumptionClaim.IN_PROGRESS) {
      return CompletableFuture.completedFuture(
          SideEventConsumptionResult.skippedInProgress(event.eventId()));
    }
    CompletionStage<Void> handled;
    try {
      handled = handler.handle(event);
    } catch (RuntimeException failure) {
      return releaseAfterFailure(event, failure);
    }
    return handled
        .thenCompose(ignored -> store.markConsumed(event.eventId()))
        .thenApply(ignored -> SideEventConsumptionResult.processed(event.eventId()))
        .exceptionallyCompose(failure -> releaseAfterFailure(event, failure));
  }

  private CompletionStage<SideEventConsumptionResult> releaseAfterFailure(
      SideEvent event, Throwable failure) {
    return store
        .release(event.eventId())
        .handle(
            (ignored, releaseFailure) -> {
              if (releaseFailure != null) {
                failure.addSuppressed(releaseFailure);
              }
              return failure;
            })
        .thenCompose(CompletableFuture::failedFuture);
  }
}
