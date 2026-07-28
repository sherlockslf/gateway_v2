package com.usig.api.resource;

import com.usig.api.dto.ConfirmImageUploadRequest;
import com.usig.api.dto.ConfirmImageUploadResponse;
import com.usig.api.dto.ImageUploadCredentialRequest;
import com.usig.api.dto.ImageUploadCredentialResponse;
import com.usig.api.dto.ServiceCapabilitiesResponse;
import com.usig.application.usecase.ConfirmImageUploadUseCase;
import com.usig.application.usecase.GetServiceCapabilitiesUseCase;
import com.usig.application.usecase.ListServicesUseCase;
import com.usig.application.usecase.RequestImageUploadCredentialUseCase;
import com.usig.domain.run.TenantId;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Path("/v1/services")
public final class ServiceResource {

  private final GetServiceCapabilitiesUseCase getServiceCapabilities;
  private final ListServicesUseCase listServices;
  private final RequestImageUploadCredentialUseCase requestImageUploadCredential;
  private final ConfirmImageUploadUseCase confirmImageUpload;

  @Inject
  public ServiceResource(
      GetServiceCapabilitiesUseCase getServiceCapabilities,
      ListServicesUseCase listServices,
      RequestImageUploadCredentialUseCase requestImageUploadCredential,
      ConfirmImageUploadUseCase confirmImageUpload) {
    this.getServiceCapabilities = getServiceCapabilities;
    this.listServices = listServices;
    this.requestImageUploadCredential = requestImageUploadCredential;
    this.confirmImageUpload = confirmImageUpload;
  }

  @GET
  public CompletionStage<List<ServiceCapabilitiesResponse>> list(
      @HeaderParam("X-Tenant-Id") String tenantId) {
    return listServices
        .list(new TenantId(tenantId))
        .thenApply(services -> services.stream().map(ServiceCapabilitiesResponse::from).toList());
  }

  @GET
  @Path("/{serviceId}/capabilities")
  public CompletionStage<ServiceCapabilitiesResponse> capabilities(
      @PathParam("serviceId") String serviceId, @HeaderParam("X-Tenant-Id") String tenantId) {
    return getServiceCapabilities
        .get(serviceId, new TenantId(tenantId))
        .thenApply(ServiceCapabilitiesResponse::from);
  }

  @POST
  @Path("/{serviceId}/image-upload-credential")
  public CompletionStage<ImageUploadCredentialResponse> imageUploadCredential(
      @PathParam("serviceId") String serviceId, ImageUploadCredentialRequest request) {
    return requestImageUploadCredential
        .request(request.toCommand(serviceId))
        .thenApply(ImageUploadCredentialResponse::from);
  }

  @POST
  @Path("/{serviceId}/image-upload-callback")
  public CompletionStage<ConfirmImageUploadResponse> imageUploadCallback(
      @PathParam("serviceId") String serviceId,
      @HeaderParam("X-USIG-Upload-Callback-Signature") String callbackSignature,
      ConfirmImageUploadRequest request) {
    return confirmImageUpload
        .confirm(request.toCommand(serviceId, callbackSignature))
        .thenApply(ConfirmImageUploadResponse::from);
  }
}
