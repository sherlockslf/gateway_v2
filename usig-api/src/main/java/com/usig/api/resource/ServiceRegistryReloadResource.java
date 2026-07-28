package com.usig.api.resource;

import com.usig.api.dto.ReloadServiceRegistryRequest;
import com.usig.api.dto.ReloadServiceRegistryResponse;
import com.usig.api.dto.ServiceRegistryReloadStatusResponse;
import com.usig.application.usecase.GetServiceRegistryReloadStatusUseCase;
import com.usig.application.usecase.ReloadServiceRegistryUseCase;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

@Path("/v1/services:reload")
public final class ServiceRegistryReloadResource {

  private final ReloadServiceRegistryUseCase reloadServiceRegistry;
  private final GetServiceRegistryReloadStatusUseCase getReloadStatus;

  @Inject
  public ServiceRegistryReloadResource(
      ReloadServiceRegistryUseCase reloadServiceRegistry,
      GetServiceRegistryReloadStatusUseCase getReloadStatus) {
    this.reloadServiceRegistry = reloadServiceRegistry;
    this.getReloadStatus = getReloadStatus;
  }

  @POST
  public ReloadServiceRegistryResponse reload(
      @HeaderParam("X-USIG-Admin-Token") String adminToken, ReloadServiceRegistryRequest request) {
    return ReloadServiceRegistryResponse.from(
        reloadServiceRegistry.reload(adminToken, request.toServices()));
  }

  @GET
  @Path("/status")
  public ServiceRegistryReloadStatusResponse status(
      @HeaderParam("X-USIG-Admin-Token") String adminToken) {
    return ServiceRegistryReloadStatusResponse.from(getReloadStatus.get(adminToken));
  }
}
