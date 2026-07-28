package com.usig.application.command;

import com.usig.domain.run.TenantId;

public record RequestImageUploadCredentialCommand(
    String serviceId,
    TenantId tenantId,
    String filename,
    String mimeType,
    long contentLengthBytes) {}
