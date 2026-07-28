package com.usig.infra.objectstorage;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

public interface S3CompatibleObjectDeletionClient {

  CompletionStage<Void> deleteObject(String objectKey, Instant now);
}
