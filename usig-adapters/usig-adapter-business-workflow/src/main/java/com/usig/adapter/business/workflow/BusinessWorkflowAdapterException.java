package com.usig.adapter.business.workflow;

import com.usig.domain.error.ErrorCode;
import com.usig.domain.error.ErrorCoded;

public final class BusinessWorkflowAdapterException extends RuntimeException implements ErrorCoded {

  private final ErrorCode errorCode;

  public BusinessWorkflowAdapterException(String message) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message, null);
  }

  public BusinessWorkflowAdapterException(ErrorCode errorCode, String message) {
    this(errorCode, message, null);
  }

  public BusinessWorkflowAdapterException(String message, Throwable cause) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message, cause);
  }

  public BusinessWorkflowAdapterException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public ErrorCode errorCode() {
    return errorCode;
  }
}
