package com.usig.application.port;

import com.usig.application.result.ImageUploadCleanupResult;
import java.util.concurrent.CompletionStage;

public interface ImageUploadLifecycleCleaner {

  CompletionStage<ImageUploadCleanupResult> cleanupExpiredUploads();
}
