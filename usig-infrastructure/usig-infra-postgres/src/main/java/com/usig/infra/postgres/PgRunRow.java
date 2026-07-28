package com.usig.infra.postgres;

import java.time.Instant;

record PgRunRow(
    String runId,
    String conversationId,
    String tenantId,
    String serviceId,
    String status,
    String executionHandleJson,
    long version,
    Instant createdAt,
    Instant updatedAt) {}
