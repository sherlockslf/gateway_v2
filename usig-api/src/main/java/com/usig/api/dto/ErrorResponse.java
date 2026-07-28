package com.usig.api.dto;

import com.usig.domain.error.ErrorCode;
import java.util.Map;

public record ErrorResponse(
    ErrorCode code,
    String message,
    boolean retryable,
    String runId,
    String serviceId,
    Map<String, Object> details) {

  public static ErrorResponse of(ErrorCode code, String message) {
    return new ErrorResponse(code, message, false, null, null, Map.of());
  }
}
