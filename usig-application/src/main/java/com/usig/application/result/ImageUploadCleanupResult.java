package com.usig.application.result;

public record ImageUploadCleanupResult(
    int removedUploadRecords, int deletedObjects, int failedObjectDeletes) {

  public ImageUploadCleanupResult(int removedUploadRecords) {
    this(removedUploadRecords, 0, 0);
  }

  public ImageUploadCleanupResult {
    if (removedUploadRecords < 0) {
      throw new IllegalArgumentException("removedUploadRecords must not be negative");
    }
    if (deletedObjects < 0) {
      throw new IllegalArgumentException("deletedObjects must not be negative");
    }
    if (failedObjectDeletes < 0) {
      throw new IllegalArgumentException("failedObjectDeletes must not be negative");
    }
  }
}
