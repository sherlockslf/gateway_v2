package com.usig.api.resource;

import com.usig.api.dto.CancelRunResponse;
import com.usig.api.dto.CreateRunRequest;
import com.usig.api.dto.GatewayEventDto;
import com.usig.api.dto.RunResponse;
import com.usig.api.dto.SubmitInputRequest;
import com.usig.api.dto.SubmitInputResponse;
import com.usig.api.sse.FlowMultiBridge;
import com.usig.application.usecase.CancelRunUseCase;
import com.usig.application.usecase.CreateRunUseCase;
import com.usig.application.usecase.GetRunUseCase;
import com.usig.application.usecase.SubmitInputUseCase;
import com.usig.application.usecase.SubscribeRunEventsUseCase;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.jboss.resteasy.reactive.RestStreamElementType;

@Path("/v1/runs")
public final class RunResource {

  private final CreateRunUseCase createRun;
  private final GetRunUseCase getRun;
  private final CancelRunUseCase cancelRun;
  private final SubmitInputUseCase submitInput;
  private final SubscribeRunEventsUseCase subscribeRunEvents;
  private final Sse sse;

  @Inject
  public RunResource(
      CreateRunUseCase createRun,
      GetRunUseCase getRun,
      CancelRunUseCase cancelRun,
      SubmitInputUseCase submitInput,
      SubscribeRunEventsUseCase subscribeRunEvents,
      Sse sse) {
    this.createRun = createRun;
    this.getRun = getRun;
    this.cancelRun = cancelRun;
    this.submitInput = submitInput;
    this.subscribeRunEvents = subscribeRunEvents;
    this.sse = sse;
  }

  @POST
  public CompletionStage<Response> create(
      CreateRunRequest request, @HeaderParam("Idempotency-Key") String idempotencyKey) {
    return createRun
        .create(request.toCommand(idempotencyKey))
        .thenApply(
            result ->
                Response.accepted(RunResponse.from(result.run(), result.eventsUrl())).build());
  }

  @GET
  @Path("/{runId}")
  public CompletionStage<RunResponse> get(
      @PathParam("runId") String runId, @HeaderParam("X-Tenant-Id") String tenantId) {
    return getRun
        .get(new RunId(runId), new TenantId(tenantId))
        .thenApply(run -> RunResponse.from(run, "/v1/runs/" + run.id().value() + "/events"));
  }

  @POST
  @Path("/{runId}:cancel")
  public CompletionStage<CancelRunResponse> cancel(
      @PathParam("runId") String runId, @HeaderParam("X-Tenant-Id") String tenantId) {
    return cancelRun
        .cancel(new RunId(runId), new TenantId(tenantId))
        .thenApply(CancelRunResponse::from);
  }

  @POST
  @Path("/{runId}:input")
  public CompletionStage<SubmitInputResponse> submitInput(
      @PathParam("runId") String runId,
      @HeaderParam("X-Tenant-Id") String tenantId,
      SubmitInputRequest request) {
    return submitInput
        .submit(request.toCommand(runId, tenantId))
        .thenApply(ignored -> SubmitInputResponse.accepted(runId));
  }

  @GET
  @Path("/{runId}/events")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  @RestStreamElementType(MediaType.APPLICATION_JSON)
  public Multi<OutboundSseEvent> events(
      @PathParam("runId") String runId,
      @HeaderParam("X-Tenant-Id") String tenantId,
      @HeaderParam("Last-Event-ID") String lastEventId) {
    CompletionStage<Flow.Publisher<GatewayEvent>> publisher =
        subscribeRunEvents.subscribe(new RunId(runId), new TenantId(tenantId), lastEventId);
    return Multi.createFrom()
        .completionStage(publisher)
        .onItem()
        .transformToMultiAndConcatenate(FlowMultiBridge::toMulti)
        .map(this::toSseEvent);
  }

  private OutboundSseEvent toSseEvent(GatewayEvent event) {
    return sse.newEventBuilder()
        .id(event.eventId())
        .name(event.wireEventType())
        .mediaType(MediaType.APPLICATION_JSON_TYPE)
        .data(GatewayEventDto.class, GatewayEventDto.from(event))
        .build();
  }
}
