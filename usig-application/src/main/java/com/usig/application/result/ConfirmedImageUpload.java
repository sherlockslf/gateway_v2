package com.usig.application.result;

import java.time.Instant;

public record ConfirmedImageUpload(
    String objectUrl, String mimeType, long contentLengthBytes, Instant retentionExpiresAt) {}
