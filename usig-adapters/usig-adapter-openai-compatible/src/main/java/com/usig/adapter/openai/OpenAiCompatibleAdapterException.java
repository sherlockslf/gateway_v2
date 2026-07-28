package com.usig.adapter.openai;

import com.usig.domain.error.ErrorCode;
import com.usig.domain.error.ErrorCoded;

public final class OpenAiCompatibleAdapterException extends RuntimeException implements ErrorCoded {

  private final ErrorCode errorCode;

  public OpenAiCompatibleAdapterException(String message) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message);
  }

  public OpenAiCompatibleAdapterException(String message, Throwable cause) {
    this(ErrorCode.DOWNSTREAM_PROTOCOL_ERROR, message, cause);
  }

  public OpenAiCompatibleAdapterException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public OpenAiCompatibleAdapterException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public ErrorCode errorCode() {
    return errorCode;
  }
}
