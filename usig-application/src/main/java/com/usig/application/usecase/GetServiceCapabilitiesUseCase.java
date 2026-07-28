package com.usig.application.usecase;

import com.usig.application.port.ServiceRegistry;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class GetServiceCapabilitiesUseCase {

  private final ServiceRegistry serviceRegistry;

  public GetServiceCapabilitiesUseCase(ServiceRegistry serviceRegistry) {
    this.serviceRegistry =
        Objects.requireNonNull(serviceRegistry, "serviceRegistry must not be null");
  }

  public CompletionStage<ServiceDescriptor> get(String serviceId, TenantId tenantId) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    return serviceRegistry
        .find(serviceId)
        .map(service -> visibleToTenant(service, tenantId))
        .orElseGet(
            () ->
                CompletableFuture.failedFuture(new UseCaseException(ErrorCode.SERVICE_NOT_FOUND)));
  }

  private static CompletionStage<ServiceDescriptor> visibleToTenant(
      ServiceDescriptor service, TenantId tenantId) {
    if (!service.visibleToTenant(tenantId.value())) {
      return CompletableFuture.failedFuture(new UseCaseException(ErrorCode.FORBIDDEN));
    }
    return CompletableFuture.completedFuture(service);
  }
}
