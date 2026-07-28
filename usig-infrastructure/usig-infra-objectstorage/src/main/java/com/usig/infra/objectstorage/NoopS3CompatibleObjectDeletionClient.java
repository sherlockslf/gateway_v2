package com.usig.infra.objectstorage;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class NoopS3CompatibleObjectDeletionClient
    implements S3CompatibleObjectDeletionClient {

  @Override
  public CompletionStage<Void> deleteObject(String objectKey, Instant now) {
    return CompletableFuture.completedFuture(null);
  }
}
