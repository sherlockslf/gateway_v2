package com.usig.application.usecase;

import com.usig.application.port.ServiceRegistry;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class ListServicesUseCase {

  private final ServiceRegistry serviceRegistry;

  public ListServicesUseCase(ServiceRegistry serviceRegistry) {
    this.serviceRegistry =
        Objects.requireNonNull(serviceRegistry, "serviceRegistry must not be null");
  }

  public CompletionStage<List<ServiceDescriptor>> list(TenantId tenantId) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    List<ServiceDescriptor> visibleServices =
        serviceRegistry.list().stream()
            .filter(service -> service.visibleToTenant(tenantId.value()))
            .sorted(Comparator.comparing(ServiceDescriptor::serviceId))
            .toList();
    return CompletableFuture.completedFuture(visibleServices);
  }
}
