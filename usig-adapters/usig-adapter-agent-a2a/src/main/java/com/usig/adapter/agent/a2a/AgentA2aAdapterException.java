package com.usig.adapter.agent.a2a;

import com.usig.domain.error.ErrorCode;
import com.usig.domain.error.ErrorCoded;

public final class AgentA2aAdapterException extends RuntimeException implements ErrorCoded {

  private final ErrorCode errorCode;

  public AgentA2aAdapterException(String message) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message);
  }

  public AgentA2aAdapterException(String message, Throwable cause) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message, cause);
  }

  public AgentA2aAdapterException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public AgentA2aAdapterException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public ErrorCode errorCode() {
    return errorCode;
  }
}
