package com.usig.api.error;

import com.usig.api.dto.ErrorResponse;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public final class UseCaseExceptionMapper implements ExceptionMapper<UseCaseException> {

  @Override
  public Response toResponse(UseCaseException exception) {
    return Response.status(statusFor(exception.errorCode()))
        .entity(ErrorResponse.of(exception.errorCode(), exception.getMessage()))
        .build();
  }

  private static Response.Status statusFor(ErrorCode code) {
    return switch (code) {
      case UNAUTHORIZED -> Response.Status.UNAUTHORIZED;
      case FORBIDDEN -> Response.Status.FORBIDDEN;
      case RUN_NOT_FOUND, SERVICE_NOT_FOUND -> Response.Status.NOT_FOUND;
      case RUN_ALREADY_TERMINAL,
          SERVICE_CAPABILITY_UNSUPPORTED,
          CANCEL_NOT_SUPPORTED,
          RESUME_NOT_SUPPORTED ->
          Response.Status.CONFLICT;
      case TENANT_LIMIT_EXCEEDED -> Response.Status.TOO_MANY_REQUESTS;
      case DOWNSTREAM_CIRCUIT_OPEN -> Response.Status.SERVICE_UNAVAILABLE;
      case INVALID_REQUEST, ADAPTER_NOT_FOUND -> Response.Status.BAD_REQUEST;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    };
  }
}
