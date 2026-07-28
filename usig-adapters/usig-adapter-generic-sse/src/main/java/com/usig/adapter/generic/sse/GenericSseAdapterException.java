package com.usig.adapter.generic.sse;

import com.usig.domain.error.ErrorCode;
import com.usig.domain.error.ErrorCoded;

public final class GenericSseAdapterException extends RuntimeException implements ErrorCoded {

  private final ErrorCode errorCode;

  public GenericSseAdapterException(String message) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message);
  }

  public GenericSseAdapterException(String message, Throwable cause) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message, cause);
  }

  public GenericSseAdapterException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public GenericSseAdapterException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public ErrorCode errorCode() {
    return errorCode;
  }
}
