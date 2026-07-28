package com.usig.application.usecase;

import com.usig.domain.error.ErrorCode;
import com.usig.domain.error.ErrorCoded;

public final class UseCaseException extends RuntimeException implements ErrorCoded {

  private final ErrorCode errorCode;

  public UseCaseException(ErrorCode errorCode) {
    super(errorCode.name());
    this.errorCode = errorCode;
  }

  public UseCaseException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }
}
