package com.usig.infra.postgres;

import com.usig.domain.run.ExecutionHandle;
import java.util.Map;

public record PostgresExecutionHandleRecord(
    String serviceId,
    String adapterId,
    String downstreamTaskId,
    String downstreamThreadId,
    String downstreamRunId,
    Map<String, String> opaqueAttributes) {

  public PostgresExecutionHandleRecord {
    opaqueAttributes = Map.copyOf(opaqueAttributes == null ? Map.of() : opaqueAttributes);
  }

  static PostgresExecutionHandleRecord fromDomain(ExecutionHandle handle) {
    if (handle == null) {
      return null;
    }
    return new PostgresExecutionHandleRecord(
        handle.serviceId(),
        handle.adapterId(),
        handle.downstreamTaskId(),
        handle.downstreamThreadId(),
        handle.downstreamRunId(),
        handle.opaqueAttributes());
  }

  ExecutionHandle toDomain() {
    return new ExecutionHandle(
        serviceId,
        adapterId,
        downstreamTaskId,
        downstreamThreadId,
        downstreamRunId,
        opaqueAttributes);
  }
}
