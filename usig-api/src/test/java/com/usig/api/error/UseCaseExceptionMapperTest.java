package com.usig.api.error;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class UseCaseExceptionMapperTest {

  private final UseCaseExceptionMapper mapper = new UseCaseExceptionMapper();

  @Test
  void mapsConcurrencyLimitToTooManyRequests() {
    Response response = mapper.toResponse(new UseCaseException(ErrorCode.TENANT_LIMIT_EXCEEDED));

    assertEquals(Response.Status.TOO_MANY_REQUESTS.getStatusCode(), response.getStatus());
  }

  @Test
  void mapsOpenCircuitToServiceUnavailable() {
    Response response = mapper.toResponse(new UseCaseException(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN));

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  @Test
  void mapsAdapterNotFoundToBadRequest() {
    Response response = mapper.toResponse(new UseCaseException(ErrorCode.ADAPTER_NOT_FOUND));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
