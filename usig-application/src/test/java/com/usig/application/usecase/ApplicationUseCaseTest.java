package com.usig.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.command.ConfirmImageUploadCommand;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.application.command.RequestImageUploadCredentialCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.memory.FixedClockPort;
import com.usig.application.memory.InMemoryImageUploadCredentialIssuer;
import com.usig.application.memory.InMemoryRunCircuitBreaker;
import com.usig.application.memory.InMemoryRunConcurrencyLimiter;
import com.usig.application.memory.InMemoryRunEventPublisher;
import com.usig.application.memory.InMemoryRunIdGenerator;
import com.usig.application.memory.InMemoryRunRepository;
import com.usig.application.memory.InMemoryServiceRegistry;
import com.usig.application.memory.NoopRunCircuitBreaker;
import com.usig.application.memory.NoopRunConcurrencyLimiter;
import com.usig.application.memory.NoopRunTelemetry;
import com.usig.application.memory.StaticGatewayEventPublisher;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.ImageUploadCredentialIssuer;
import com.usig.application.result.ConfirmedImageUpload;
import com.usig.application.result.CreateRunResult;
import com.usig.application.result.ImageUploadCredential;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationUseCaseTest {

  private static final Instant NOW = Instant.parse("2026-07-27T03:00:00Z");

  private FixedClockPort clock;
  private InMemoryRunRepository runRepository;
  private InMemoryServiceRegistry serviceRegistry;
  private InMemoryRunEventPublisher eventPublisher;
  private FakeDownstreamExecutionPort adapter;

  @BeforeEach
  void setUp() {
    clock = new FixedClockPort(NOW);
    runRepository = new InMemoryRunRepository(clock);
    serviceRegistry = new InMemoryServiceRegistry();
    eventPublisher = new InMemoryRunEventPublisher();
    adapter = new FakeDownstreamExecutionPort();
    serviceRegistry.register(serviceDescriptor("sample-agent", true, true, true), adapter);
  }

  @Test
  void createsRunStartsAdapterAndAttachesEvents() {
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    CreateRunResult result = useCase.create(createCommand("idem-1")).toCompletableFuture().join();

    assertEquals("run_1", result.run().id().value());
    assertEquals(RunStatus.STARTING, result.run().status());
    assertEquals("/v1/runs/run_1/events", result.eventsUrl());
    assertEquals(1, adapter.executeCount.get());
    assertEquals("task_1", result.run().executionHandle().downstreamTaskId());
    assertEquals(1, collect(eventPublisher.subscribe(result.run().id(), null)).size());
    assertEquals(
        RunStatus.RUNNING,
        runRepository
            .find(result.run().id(), result.run().tenantId())
            .toCompletableFuture()
            .join()
            .orElseThrow()
            .status());
  }

  @Test
  void createsRunWithSafeImageReference() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/png", 1024, pngPrefix()));

    CreateRunResult result =
        useCase
            .create(
                createImageCommand("idem-image", "https://cdn.example.com/img-1.png", "image/png"))
            .toCompletableFuture()
            .join();

    assertEquals("run_1", result.run().id().value());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void rejectsImageInputWhenServiceDoesNotSupportMultimodal() {
    serviceRegistry.register(
        serviceDescriptor("text-only-agent", true, true, true, false), adapter);
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "text-only-agent",
                        new TenantId("tenant_1"),
                        "idem-image",
                        "https://cdn.example.com/img-1.png",
                        "image/png")));

    assertEquals(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsUnsafeImageUrlBeforeAdapterExecution() {
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    UseCaseException httpException =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-http", "http://cdn.example.com/img-1.png", "image/png")));
    UseCaseException localhostException =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-localhost", "https://localhost/img-1.png", "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, httpException.errorCode());
    assertEquals(ErrorCode.INVALID_REQUEST, localhostException.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsUnsupportedImageMimeTypeBeforeAdapterExecution() {
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-bad-mime",
                        "https://cdn.example.com/img-1.bin",
                        "application/octet-stream")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenResolvedHostIsPrivate() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("10.0.0.12")),
            uri -> new ImageUrlProbeResult(200, "image/png", 1024));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-private-dns", "https://cdn.example.com/img-1.png", "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenRemoteContentIsTooLarge() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/png", 10L * 1024 * 1024 + 1));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-large-image", "https://cdn.example.com/img-1.png", "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenRemoteContentLengthIsUnknown() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/png", -1));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-unknown-image-size",
                        "https://cdn.example.com/img-1.png",
                        "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenRemoteRedirects() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(302, "image/png", 1024));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-redirect-image", "https://cdn.example.com/img-1.png", "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenRemoteMimeDoesNotMatchDeclaredMime() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/jpeg; charset=binary", 1024));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-mime-mismatch", "https://cdn.example.com/img-1.png", "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenContentSignatureDoesNotMatchDeclaredMime() {
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri ->
                new ImageUrlProbeResult(
                    200, "image/png", 1024, new byte[] {(byte) 0xff, (byte) 0xd8, (byte) 0xff}));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "idem-signature-mismatch",
                        "https://cdn.example.com/img-1.png",
                        "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void createsRunWithImageHostAllowedByServiceAllowlist() {
    serviceRegistry.register(
        serviceDescriptorWithAllowedImageHosts("image-allowlist-agent", Set.of("example.com")),
        adapter);
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/png", 1024, pngPrefix()));

    CreateRunResult result =
        useCase
            .create(
                createImageCommand(
                    "image-allowlist-agent",
                    new TenantId("tenant_1"),
                    "idem-image-allowlist",
                    "https://cdn.example.com/img-1.png",
                    "image/png"))
            .toCompletableFuture()
            .join();

    assertEquals("image-allowlist-agent", result.run().serviceId());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void rejectsImageUrlWhenHostIsOutsideServiceAllowlist() {
    serviceRegistry.register(
        serviceDescriptorWithAllowedImageHosts("image-allowlist-agent", Set.of("assets.example")),
        adapter);
    CreateRunUseCase useCase =
        createRunUseCaseWithImageValidation(
            host -> List.of(InetAddress.getByName("8.8.8.8")),
            uri -> new ImageUrlProbeResult(200, "image/png", 1024, pngPrefix()));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.create(
                    createImageCommand(
                        "image-allowlist-agent",
                        new TenantId("tenant_1"),
                        "idem-image-denied-host",
                        "https://cdn.example.com/img-1.png",
                        "image/png")));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
    assertEquals(0, adapter.executeCount.get());
  }

  @Test
  void requestsImageUploadCredentialForVisibleMultimodalService() {
    FakeImageUploadCredentialIssuer issuer = new FakeImageUploadCredentialIssuer();
    RequestImageUploadCredentialUseCase useCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);

    ImageUploadCredential credential =
        useCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent",
                    new TenantId("tenant_1"),
                    "photo.PNG",
                    "image/png; charset=binary",
                    1024))
            .toCompletableFuture()
            .join();

    assertEquals("https://upload.example/upload", credential.uploadUrl());
    assertEquals("sample-agent", issuer.service.serviceId());
    assertEquals("image/png", issuer.mimeType);
    assertEquals(1024, issuer.contentLengthBytes);
  }

  @Test
  void imageUploadCredentialIssuerReturnsConfiguredRetentionDeadline() {
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(clock, Duration.ofHours(2));

    ImageUploadCredential credential =
        issuer
            .issue(
                new TenantId("tenant_1"),
                serviceDescriptorWithAllowedImageHosts("sample-agent", Set.of("cdn.example.com")),
                "photo.png",
                "image/png",
                1024)
            .toCompletableFuture()
            .join();

    assertEquals(NOW.plus(Duration.ofMinutes(15)), credential.expiresAt());
    assertEquals(NOW.plus(Duration.ofHours(2)), credential.retentionExpiresAt());
  }

  @Test
  void confirmsIssuedImageUploadCompletion() {
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(clock, Duration.ofHours(2));
    RequestImageUploadCredentialUseCase requestUseCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);
    ConfirmImageUploadUseCase confirmUseCase =
        new ConfirmImageUploadUseCase(serviceRegistry, issuer);
    ImageUploadCredential credential =
        requestUseCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent", new TenantId("tenant_1"), "photo.png", "image/png", 1024))
            .toCompletableFuture()
            .join();

    ConfirmedImageUpload confirmed =
        confirmUseCase
            .confirm(
                new ConfirmImageUploadCommand(
                    "sample-agent",
                    new TenantId("tenant_1"),
                    credential.objectUrl(),
                    "image/png",
                    1024))
            .toCompletableFuture()
            .join();

    assertEquals(credential.objectUrl(), confirmed.objectUrl());
    assertEquals("image/png", confirmed.mimeType());
    assertEquals(1024, confirmed.contentLengthBytes());
    assertEquals(credential.retentionExpiresAt(), confirmed.retentionExpiresAt());
  }

  @Test
  void rejectsImageUploadCompletionWhenIssuedMetadataDoesNotMatch() {
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(clock, Duration.ofHours(2));
    RequestImageUploadCredentialUseCase requestUseCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);
    ConfirmImageUploadUseCase confirmUseCase =
        new ConfirmImageUploadUseCase(serviceRegistry, issuer);
    ImageUploadCredential credential =
        requestUseCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent", new TenantId("tenant_1"), "photo.png", "image/png", 1024))
            .toCompletableFuture()
            .join();

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                confirmUseCase.confirm(
                    new ConfirmImageUploadCommand(
                        "sample-agent",
                        new TenantId("tenant_1"),
                        credential.objectUrl(),
                        "image/jpeg",
                        1024)));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
  }

  @Test
  void rejectsImageUploadCompletionWhenCredentialExpired() {
    Instant[] now = {NOW};
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(() -> now[0], Duration.ofHours(2));
    RequestImageUploadCredentialUseCase requestUseCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);
    ConfirmImageUploadUseCase confirmUseCase =
        new ConfirmImageUploadUseCase(serviceRegistry, issuer);
    ImageUploadCredential credential =
        requestUseCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent", new TenantId("tenant_1"), "photo.png", "image/png", 1024))
            .toCompletableFuture()
            .join();
    now[0] = NOW.plus(Duration.ofMinutes(16));

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                confirmUseCase.confirm(
                    new ConfirmImageUploadCommand(
                        "sample-agent",
                        new TenantId("tenant_1"),
                        credential.objectUrl(),
                        "image/png",
                        1024)));

    assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
  }

  @Test
  void imageUploadLifecycleCleanupRemovesExpiredIssuedRecordsOnly() {
    Instant[] now = {NOW};
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(() -> now[0], Duration.ofHours(2));
    RequestImageUploadCredentialUseCase requestUseCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);
    ConfirmImageUploadUseCase confirmUseCase =
        new ConfirmImageUploadUseCase(serviceRegistry, issuer);
    ImageUploadCredential expired =
        requestUseCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent", new TenantId("tenant_1"), "expired.png", "image/png", 1024))
            .toCompletableFuture()
            .join();
    now[0] = NOW.plus(Duration.ofMinutes(16));
    ImageUploadCredential active =
        requestUseCase
            .request(
                new RequestImageUploadCredentialCommand(
                    "sample-agent", new TenantId("tenant_1"), "active.png", "image/png", 2048))
            .toCompletableFuture()
            .join();

    assertEquals(
        1, issuer.cleanupExpiredUploads().toCompletableFuture().join().removedUploadRecords());
    UseCaseException expiredException =
        assertThrows(
            UseCaseException.class,
            () ->
                confirmUseCase.confirm(
                    new ConfirmImageUploadCommand(
                        "sample-agent",
                        new TenantId("tenant_1"),
                        expired.objectUrl(),
                        "image/png",
                        1024)));
    ConfirmedImageUpload confirmed =
        confirmUseCase
            .confirm(
                new ConfirmImageUploadCommand(
                    "sample-agent",
                    new TenantId("tenant_1"),
                    active.objectUrl(),
                    "image/png",
                    2048))
            .toCompletableFuture()
            .join();

    assertEquals(ErrorCode.INVALID_REQUEST, expiredException.errorCode());
    assertEquals(active.objectUrl(), confirmed.objectUrl());
  }

  @Test
  void imageUploadLifecycleCleanupRemovesRetentionExpiredRecords() {
    Instant[] now = {NOW};
    InMemoryImageUploadCredentialIssuer issuer =
        new InMemoryImageUploadCredentialIssuer(() -> now[0], Duration.ofMinutes(20));
    ImageUploadCredential credential =
        issuer
            .issue(
                new TenantId("tenant_1"),
                serviceDescriptorWithAllowedImageHosts("sample-agent", Set.of("cdn.example.com")),
                "photo.png",
                "image/png",
                1024)
            .toCompletableFuture()
            .join();
    now[0] = credential.retentionExpiresAt();

    assertEquals(
        1, issuer.cleanupExpiredUploads().toCompletableFuture().join().removedUploadRecords());
    assertEquals(
        0, issuer.cleanupExpiredUploads().toCompletableFuture().join().removedUploadRecords());
  }

  @Test
  void rejectsImageUploadCredentialWhenServiceDoesNotSupportMultimodal() {
    serviceRegistry.register(
        serviceDescriptor("text-only-agent", true, true, true, false), adapter);
    FakeImageUploadCredentialIssuer issuer = new FakeImageUploadCredentialIssuer();
    RequestImageUploadCredentialUseCase useCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.request(
                    new RequestImageUploadCredentialCommand(
                        "text-only-agent",
                        new TenantId("tenant_1"),
                        "photo.png",
                        "image/png",
                        1024)));

    assertEquals(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED, exception.errorCode());
    assertEquals(0, issuer.issueCount.get());
  }

  @Test
  void rejectsImageUploadCredentialWhenMimeOrSizeIsInvalid() {
    FakeImageUploadCredentialIssuer issuer = new FakeImageUploadCredentialIssuer();
    RequestImageUploadCredentialUseCase useCase =
        new RequestImageUploadCredentialUseCase(serviceRegistry, issuer);

    UseCaseException mimeException =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.request(
                    new RequestImageUploadCredentialCommand(
                        "sample-agent",
                        new TenantId("tenant_1"),
                        "photo.svg",
                        "image/svg+xml",
                        1024)));
    UseCaseException sizeException =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase.request(
                    new RequestImageUploadCredentialCommand(
                        "sample-agent",
                        new TenantId("tenant_1"),
                        "photo.png",
                        "image/png",
                        10L * 1024 * 1024 + 1)));

    assertEquals(ErrorCode.INVALID_REQUEST, mimeException.errorCode());
    assertEquals(ErrorCode.INVALID_REQUEST, sizeException.errorCode());
    assertEquals(0, issuer.issueCount.get());
  }

  @Test
  void inputRequiredEventMovesRunToWaitingInputBeforeSubmit() {
    adapter.events =
        List.of(
            event("evt_1", EventType.EXECUTION_STARTED, 0, Map.of()),
            event("evt_2", EventType.INPUT_REQUIRED, 1, Map.of("prompt", "approve?")));
    CreateRunUseCase createRun =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);
    CreateRunResult result = createRun.create(createCommand("idem-1")).toCompletableFuture().join();

    List<GatewayEvent> events = collect(eventPublisher.subscribe(result.run().id(), null));

    assertEquals(2, events.size());
    assertEquals(EventType.INPUT_REQUIRED, events.get(1).eventType());
    assertEquals(
        RunStatus.WAITING_INPUT,
        runRepository
            .find(result.run().id(), result.run().tenantId())
            .toCompletableFuture()
            .join()
            .orElseThrow()
            .status());
    new SubmitInputUseCase(runRepository, serviceRegistry)
        .submit(
            new SubmitInputCommand(
                result.run().id(), result.run().tenantId(), Map.of("answer", "approved")))
        .toCompletableFuture()
        .join();
    assertEquals(1, adapter.submitInputCount.get());
  }

  @Test
  void approvalRequiredEventMovesRunToWaitingInputBeforeSubmit() {
    adapter.events =
        List.of(
            event("evt_1", EventType.EXECUTION_STARTED, 0, Map.of()),
            event(
                "evt_2",
                EventType.APPROVAL_REQUIRED,
                1,
                Map.of("approvalRequestId", "appr_1", "name", "deploy")));
    CreateRunUseCase createRun =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);
    CreateRunResult result = createRun.create(createCommand("idem-1")).toCompletableFuture().join();

    List<GatewayEvent> events = collect(eventPublisher.subscribe(result.run().id(), null));

    assertEquals(2, events.size());
    assertEquals(EventType.APPROVAL_REQUIRED, events.get(1).eventType());
    assertEquals(
        RunStatus.WAITING_INPUT,
        runRepository
            .find(result.run().id(), result.run().tenantId())
            .toCompletableFuture()
            .join()
            .orElseThrow()
            .status());
    new SubmitInputUseCase(runRepository, serviceRegistry)
        .submit(
            new SubmitInputCommand(
                result.run().id(),
                result.run().tenantId(),
                Map.of("approvalRequestId", "appr_1", "approve", true)))
        .toCompletableFuture()
        .join();
    assertEquals(1, adapter.submitInputCount.get());
    assertEquals("appr_1", adapter.submittedPayload.get("approvalRequestId"));
    assertEquals(true, adapter.submittedPayload.get("approve"));
  }

  @Test
  void returnsSameRunForDuplicateIdempotencyKeyWithoutExecutingAdapterAgain() {
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    CreateRunResult first = useCase.create(createCommand("idem-1")).toCompletableFuture().join();
    CreateRunResult second = useCase.create(createCommand("idem-1")).toCompletableFuture().join();

    assertEquals(first.run().id(), second.run().id());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void retriesCreateExecutionOnceWhenServiceDeclaresRetrySafety() {
    serviceRegistry.register(
        serviceDescriptorWithRetrySafety("retry-safe-agent", RetrySafety.IDEMPOTENT_CREATE_ONLY),
        adapter);
    adapter.executeFailuresBeforeSuccess.set(1);
    CreateRunUseCase useCase =
        createRunUseCaseWithRetryPolicy(CreateRunRetryPolicy.immediateForTests());

    CreateRunResult result =
        useCase
            .create(createCommand("retry-safe-agent", new TenantId("tenant_1"), "idem-retry"))
            .toCompletableFuture()
            .join();

    assertEquals("retry-safe-agent", result.run().serviceId());
    assertEquals(2, adapter.executeCount.get());
  }

  @Test
  void doesNotRetryCreateExecutionWhenServiceRetrySafetyIsNone() {
    serviceRegistry.register(
        serviceDescriptorWithRetrySafety("no-retry-agent", RetrySafety.NONE), adapter);
    adapter.executeFailuresBeforeSuccess.set(1);
    CreateRunUseCase useCase =
        createRunUseCaseWithRetryPolicy(CreateRunRetryPolicy.immediateForTests());

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("no-retry-agent", new TenantId("tenant_1"), "idem-none"))
                    .toCompletableFuture()
                    .join());

    assertEquals("execute failed", exception.getCause().getMessage());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void retryPolicyAddsBoundedJitterDelay() {
    CreateRunRetryPolicy policy =
        new CreateRunRetryPolicy(2, Duration.ofMillis(10), Duration.ofMillis(40));

    Duration delay = policy.delayBeforeRetry(1);

    assertTrue(!delay.minusMillis(10).isNegative());
    assertTrue(!Duration.ofMillis(50).minus(delay).isNegative());
  }

  @Test
  void rejectsSameTenantServiceRunWhenTenantServiceConcurrencyIsExceeded() {
    serviceRegistry.register(serviceDescriptor("limited-agent", true, true, true, 2, 1), adapter);
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new com.usig.application.memory.NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter());

    CreateRunResult first =
        useCase
            .create(createCommand("limited-agent", new TenantId("tenant_1"), "idem-1"))
            .toCompletableFuture()
            .join();
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("limited-agent", new TenantId("tenant_1"), "idem-2"))
                    .toCompletableFuture()
                    .join());
    collect(eventPublisher.subscribe(first.run().id(), null));
    CreateRunResult afterRelease =
        useCase
            .create(createCommand("limited-agent", new TenantId("tenant_1"), "idem-3"))
            .toCompletableFuture()
            .join();

    assertEquals(
        ErrorCode.TENANT_LIMIT_EXCEEDED, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals("run_3", afterRelease.run().id().value());
    assertEquals(2, adapter.executeCount.get());
  }

  @Test
  void rejectsRunWhenServiceConcurrencyIsExceededAcrossTenants() {
    serviceRegistry.register(
        serviceDescriptor("shared-agent", true, true, true, Set.of("tenant_1", "tenant_2"), 1, 1),
        adapter);
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new com.usig.application.memory.NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter());

    useCase
        .create(createCommand("shared-agent", new TenantId("tenant_1"), "idem-1"))
        .toCompletableFuture()
        .join();
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("shared-agent", new TenantId("tenant_2"), "idem-2"))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.TENANT_LIMIT_EXCEEDED, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void rejectsRunWhenServiceGroupConcurrencyIsExceededAcrossServices() {
    serviceRegistry.register(
        serviceDescriptor("group-agent-a", true, true, true, "shared-group", 5, 5, 1), adapter);
    serviceRegistry.register(
        serviceDescriptor("group-agent-b", true, true, true, "shared-group", 5, 5, 1), adapter);
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter());

    CreateRunResult first =
        useCase
            .create(createCommand("group-agent-a", new TenantId("tenant_1"), "idem-1"))
            .toCompletableFuture()
            .join();
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("group-agent-b", new TenantId("tenant_1"), "idem-2"))
                    .toCompletableFuture()
                    .join());
    collect(eventPublisher.subscribe(first.run().id(), null));
    CreateRunResult afterRelease =
        useCase
            .create(createCommand("group-agent-b", new TenantId("tenant_1"), "idem-3"))
            .toCompletableFuture()
            .join();

    assertEquals(
        ErrorCode.TENANT_LIMIT_EXCEEDED, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals("group-agent-b", afterRelease.run().serviceId());
    assertEquals(2, adapter.executeCount.get());
  }

  @Test
  void opensServiceCircuitAfterDownstreamStreamFailure() {
    serviceRegistry.register(
        serviceDescriptor(
            "fast-break-agent",
            true,
            true,
            true,
            null,
            500,
            100,
            500,
            new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(30))),
        adapter);
    adapter.eventsPublisherOverride =
        subscriber -> {
          subscriber.onSubscribe(
              new Flow.Subscription() {
                @Override
                public void request(long n) {
                  subscriber.onError(new IllegalStateException("downstream failed"));
                }

                @Override
                public void cancel() {}
              });
        };
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter(),
            new InMemoryRunCircuitBreaker(clock));

    CreateRunResult first =
        useCase
            .create(createCommand("fast-break-agent", new TenantId("tenant_1"), "idem-1"))
            .toCompletableFuture()
            .join();
    CompletionException streamFailure =
        assertThrows(
            CompletionException.class,
            () -> collect(eventPublisher.subscribe(first.run().id(), null)));
    CompletionException circuitOpen =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("fast-break-agent", new TenantId("tenant_1"), "idem-2"))
                    .toCompletableFuture()
                    .join());

    assertEquals("downstream failed", streamFailure.getCause().getMessage());
    assertEquals(
        ErrorCode.DOWNSTREAM_CIRCUIT_OPEN, ((UseCaseException) circuitOpen.getCause()).errorCode());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void opensServiceCircuitOnlyAfterConfiguredFailureThreshold() {
    serviceRegistry.register(
        serviceDescriptor(
            "two-failure-agent",
            true,
            true,
            true,
            null,
            500,
            100,
            500,
            new ServiceCircuitBreakerPolicy(2, Duration.ofSeconds(45))),
        adapter);
    adapter.eventsPublisherOverride =
        subscriber -> {
          subscriber.onSubscribe(
              new Flow.Subscription() {
                @Override
                public void request(long n) {
                  subscriber.onError(new IllegalStateException("downstream failed"));
                }

                @Override
                public void cancel() {}
              });
        };
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter(),
            new InMemoryRunCircuitBreaker(clock));

    CreateRunResult first =
        useCase
            .create(createCommand("two-failure-agent", new TenantId("tenant_1"), "idem-1"))
            .toCompletableFuture()
            .join();
    assertThrows(
        CompletionException.class, () -> collect(eventPublisher.subscribe(first.run().id(), null)));
    CreateRunResult second =
        useCase
            .create(createCommand("two-failure-agent", new TenantId("tenant_1"), "idem-2"))
            .toCompletableFuture()
            .join();
    assertThrows(
        CompletionException.class,
        () -> collect(eventPublisher.subscribe(second.run().id(), null)));
    CompletionException circuitOpen =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("two-failure-agent", new TenantId("tenant_1"), "idem-3"))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.DOWNSTREAM_CIRCUIT_OPEN, ((UseCaseException) circuitOpen.getCause()).errorCode());
    assertEquals(2, adapter.executeCount.get());
  }

  @Test
  void opensServiceGroupCircuitAfterDownstreamStreamFailure() {
    serviceRegistry.register(
        serviceDescriptor(
            "group-agent-a",
            true,
            true,
            true,
            "shared-group",
            5,
            5,
            5,
            new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(30))),
        adapter);
    serviceRegistry.register(
        serviceDescriptor(
            "group-agent-b",
            true,
            true,
            true,
            "shared-group",
            5,
            5,
            5,
            new ServiceCircuitBreakerPolicy(1, Duration.ofSeconds(30))),
        adapter);
    adapter.eventsPublisherOverride =
        subscriber -> {
          subscriber.onSubscribe(
              new Flow.Subscription() {
                @Override
                public void request(long n) {
                  subscriber.onError(new IllegalStateException("downstream failed"));
                }

                @Override
                public void cancel() {}
              });
        };
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository,
            serviceRegistry,
            eventPublisher,
            new InMemoryRunIdGenerator(),
            clock,
            new NoopRunTelemetry(),
            new InMemoryRunConcurrencyLimiter(),
            new InMemoryRunCircuitBreaker(clock));

    CreateRunResult first =
        useCase
            .create(createCommand("group-agent-a", new TenantId("tenant_1"), "idem-1"))
            .toCompletableFuture()
            .join();
    assertThrows(
        CompletionException.class, () -> collect(eventPublisher.subscribe(first.run().id(), null)));
    CompletionException circuitOpen =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .create(createCommand("group-agent-b", new TenantId("tenant_1"), "idem-2"))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.DOWNSTREAM_CIRCUIT_OPEN, ((UseCaseException) circuitOpen.getCause()).errorCode());
    assertEquals(1, adapter.executeCount.get());
  }

  @Test
  void rejectsInvisibleTenantWhenCreatingRun() {
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            runRepository, serviceRegistry, eventPublisher, new InMemoryRunIdGenerator(), clock);

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                useCase
                    .create(
                        new CreateRunCommand(
                            new ConversationId("conv_1"),
                            new TenantId("tenant_2"),
                            "sample-agent",
                            "idem-1",
                            List.of(),
                            Map.of()))
                    .toCompletableFuture()
                    .join());

    assertEquals(ErrorCode.FORBIDDEN, exception.errorCode());
  }

  @Test
  void getsRunWithTenantIsolation() {
    Run run = acceptedRun("run_1", "tenant_1", RunStatus.ACCEPTED, 0);
    runRepository.create(run, null).toCompletableFuture().join();
    GetRunUseCase useCase = new GetRunUseCase(runRepository);

    assertEquals(
        run,
        useCase.get(new RunId("run_1"), new TenantId("tenant_1")).toCompletableFuture().join());
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .get(new RunId("run_1"), new TenantId("tenant_2"))
                    .toCompletableFuture()
                    .join());
    assertEquals(ErrorCode.RUN_NOT_FOUND, ((UseCaseException) exception.getCause()).errorCode());
  }

  @Test
  void cancelSupportedRunMarksCancelRequestedAndCallsAdapter() {
    Run running =
        acceptedRun("run_1", "tenant_1", RunStatus.RUNNING, 0)
            .withExecutionHandle(
                new ExecutionHandle(
                    "sample-agent", "sample-adapter", "task_1", null, null, Map.of()),
                NOW);
    runRepository.create(running, null).toCompletableFuture().join();
    CancelRunUseCase useCase = new CancelRunUseCase(runRepository, serviceRegistry);

    var result =
        useCase.cancel(new RunId("run_1"), new TenantId("tenant_1")).toCompletableFuture().join();

    assertEquals(RunStatus.CANCEL_REQUESTED, result.run().status());
    assertEquals(DownstreamExecutionState.CANCELED, result.downstreamState());
    assertEquals(1, adapter.cancelCount.get());
  }

  @Test
  void cancelUnsupportedRunDoesNotCallAdapter() {
    serviceRegistry.register(serviceDescriptor("no-cancel-agent", true, false, false), adapter);
    Run running = acceptedRun("run_2", "tenant_1", RunStatus.RUNNING, 0);
    runRepository.create(running, null).toCompletableFuture().join();
    CancelRunUseCase useCase = new CancelRunUseCase(runRepository, serviceRegistry);

    var result =
        useCase.cancel(new RunId("run_2"), new TenantId("tenant_1")).toCompletableFuture().join();

    assertEquals(RunStatus.CANCEL_REQUESTED, result.run().status());
    assertEquals(DownstreamExecutionState.CANCEL_NOT_SUPPORTED, result.downstreamState());
    assertEquals(0, adapter.cancelCount.get());
  }

  @Test
  void subscribesOnlyExistingRuns() {
    Run run = acceptedRun("run_1", "tenant_1", RunStatus.ACCEPTED, 0);
    runRepository.create(run, null).toCompletableFuture().join();
    SubscribeRunEventsUseCase useCase =
        new SubscribeRunEventsUseCase(runRepository, eventPublisher, serviceRegistry);

    assertTrue(
        useCase
                .subscribe(new RunId("run_1"), new TenantId("tenant_1"), null)
                .toCompletableFuture()
                .join()
            instanceof Flow.Publisher<GatewayEvent>);
  }

  @Test
  void resumesNonTerminalRunAfterReplayWhenCursorProvided() {
    serviceRegistry.register(resumableServiceDescriptor("resumable-agent"), adapter);
    Run running =
        new Run(
                new RunId("run_resume_1"),
                new ConversationId("conv_1"),
                new TenantId("tenant_1"),
                "resumable-agent",
                RunStatus.RUNNING,
                null,
                0,
                NOW,
                NOW)
            .withExecutionHandle(
                new ExecutionHandle(
                    "resumable-agent", "sample-adapter", "task_1", null, "downstream_1", Map.of()),
                NOW);
    runRepository.create(running, null).toCompletableFuture().join();
    eventPublisher.attach(
        running.id(),
        new StaticGatewayEventPublisher(
            List.of(event("evt_replay", EventType.TEXT_DELTA, 1, Map.of("content", "cached")))));
    adapter.resumeEvents =
        List.of(event("evt_resumed", EventType.TEXT_DELTA, 2, Map.of("content", "live")));
    SubscribeRunEventsUseCase useCase =
        new SubscribeRunEventsUseCase(runRepository, eventPublisher, serviceRegistry);

    List<GatewayEvent> events =
        collect(
            useCase
                .subscribe(running.id(), running.tenantId(), "evt_seen")
                .toCompletableFuture()
                .join());

    assertEquals(
        List.of("evt_replay", "evt_resumed"), events.stream().map(GatewayEvent::eventId).toList());
    assertEquals(1, adapter.resumeCount.get());
    assertEquals("evt_seen", adapter.resumeCursor);
    assertEquals("conv_1", adapter.resumeContext.attributes().get("conversationId"));
  }

  @Test
  void retriesResumeForResumeSafeServiceBeforeAttachingResumedStream() {
    serviceRegistry.register(
        resumableServiceDescriptor("resume-safe-agent", RetrySafety.RESUME_SAFE), adapter);
    Run running =
        new Run(
                new RunId("run_resume_retry"),
                new ConversationId("conv_1"),
                new TenantId("tenant_1"),
                "resume-safe-agent",
                RunStatus.RUNNING,
                null,
                0,
                NOW,
                NOW)
            .withExecutionHandle(
                new ExecutionHandle(
                    "resume-safe-agent",
                    "sample-adapter",
                    "task_1",
                    null,
                    "downstream_1",
                    Map.of()),
                NOW);
    runRepository.create(running, null).toCompletableFuture().join();
    eventPublisher.attach(
        running.id(),
        new StaticGatewayEventPublisher(
            List.of(event("evt_replay", EventType.TEXT_DELTA, 1, Map.of("content", "cached")))));
    adapter.resumeFailuresBeforeSuccess.set(1);
    adapter.resumeEvents =
        List.of(event("evt_resumed", EventType.TEXT_DELTA, 2, Map.of("content", "live")));
    SubscribeRunEventsUseCase useCase =
        new SubscribeRunEventsUseCase(
            runRepository, eventPublisher, serviceRegistry, ResumeRetryPolicy.immediateForTests());

    List<GatewayEvent> events =
        collect(
            useCase
                .subscribe(running.id(), running.tenantId(), "evt_seen")
                .toCompletableFuture()
                .join());

    assertEquals(
        List.of("evt_replay", "evt_resumed"), events.stream().map(GatewayEvent::eventId).toList());
    assertEquals(2, adapter.resumeCount.get());
  }

  @Test
  void doesNotRetryResumeForCreateOnlyRetrySafety() {
    serviceRegistry.register(
        resumableServiceDescriptor("create-only-agent", RetrySafety.IDEMPOTENT_CREATE_ONLY),
        adapter);
    Run running =
        new Run(
                new RunId("run_resume_no_retry"),
                new ConversationId("conv_1"),
                new TenantId("tenant_1"),
                "create-only-agent",
                RunStatus.RUNNING,
                null,
                0,
                NOW,
                NOW)
            .withExecutionHandle(
                new ExecutionHandle(
                    "create-only-agent",
                    "sample-adapter",
                    "task_1",
                    null,
                    "downstream_1",
                    Map.of()),
                NOW);
    runRepository.create(running, null).toCompletableFuture().join();
    adapter.resumeFailuresBeforeSuccess.set(1);
    SubscribeRunEventsUseCase useCase =
        new SubscribeRunEventsUseCase(
            runRepository, eventPublisher, serviceRegistry, ResumeRetryPolicy.immediateForTests());

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .subscribe(running.id(), running.tenantId(), "evt_seen")
                    .toCompletableFuture()
                    .join());

    assertTrue(exception.getCause() instanceof IllegalStateException);
    assertEquals(1, adapter.resumeCount.get());
  }

  @Test
  void resumeRetryPolicyAddsBoundedJitterDelay() {
    ResumeRetryPolicy policy =
        new ResumeRetryPolicy(2, java.time.Duration.ofMillis(10), java.time.Duration.ofMillis(40));

    java.time.Duration delay = policy.delayBeforeRetry(1);

    assertTrue(delay.toMillis() >= 10);
    assertTrue(delay.toMillis() <= 50);
    assertEquals(2, policy.maxAttemptsFor(RetrySafety.RESUME_SAFE));
    assertEquals(1, policy.maxAttemptsFor(RetrySafety.IDEMPOTENT_CREATE_ONLY));
  }

  @Test
  void rejectsResumeWhenServiceDoesNotSupportResume() {
    Run running =
        acceptedRun("run_1", "tenant_1", RunStatus.RUNNING, 0)
            .withExecutionHandle(
                new ExecutionHandle(
                    "sample-agent", "sample-adapter", "task_1", null, "downstream_1", Map.of()),
                NOW);
    runRepository.create(running, null).toCompletableFuture().join();
    SubscribeRunEventsUseCase useCase =
        new SubscribeRunEventsUseCase(runRepository, eventPublisher, serviceRegistry);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .subscribe(running.id(), running.tenantId(), "evt_seen")
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.RESUME_NOT_SUPPORTED, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals(0, adapter.resumeCount.get());
  }

  @Test
  void submitsInputOnlyWhenRunIsWaitingInputAndServiceSupportsHumanInput() {
    ExecutionHandle handle =
        new ExecutionHandle("sample-agent", "sample-adapter", "task_1", null, "resp_1", Map.of());
    Run waiting =
        acceptedRun("run_1", "tenant_1", RunStatus.WAITING_INPUT, 0)
            .withExecutionHandle(handle, NOW);
    runRepository.create(waiting, null).toCompletableFuture().join();
    SubmitInputUseCase useCase = new SubmitInputUseCase(runRepository, serviceRegistry);

    useCase
        .submit(
            new SubmitInputCommand(
                new RunId("run_1"), new TenantId("tenant_1"), Map.of("answer", "approved")))
        .toCompletableFuture()
        .join();

    assertEquals(1, adapter.submitInputCount.get());
    assertEquals(Map.of("answer", "approved"), adapter.submittedPayload);
    assertEquals(handle, adapter.submittedHandle);
  }

  @Test
  void rejectsApprovalInputWhenPayloadIsInvalid() {
    ExecutionHandle handle =
        new ExecutionHandle("sample-agent", "sample-adapter", "task_1", null, "resp_1", Map.of());
    Run waiting =
        acceptedRun("run_1", "tenant_1", RunStatus.WAITING_INPUT, 0)
            .withExecutionHandle(handle, NOW);
    runRepository.create(waiting, null).toCompletableFuture().join();
    SubmitInputUseCase useCase = new SubmitInputUseCase(runRepository, serviceRegistry);

    assertInvalidInput(useCase, Map.of("approvalRequestId", "appr_1"), "approve must be a boolean");
    assertInvalidInput(
        useCase,
        Map.of("approvalRequestId", "appr_1", "approve", "true"),
        "approve must be a boolean");
    assertInvalidInput(
        useCase,
        Map.of("approvalRequestId", " ", "approve", true),
        "approvalRequestId must be a non-blank string");
    assertInvalidInput(
        useCase,
        Map.of("approvalRequestId", "appr_1", "approve", false, "reason", 1),
        "reason must be a string");
    assertEquals(0, adapter.submitInputCount.get());
  }

  @Test
  void rejectsInputWhenRunIsNotWaitingInput() {
    Run running = acceptedRun("run_1", "tenant_1", RunStatus.RUNNING, 0);
    runRepository.create(running, null).toCompletableFuture().join();
    SubmitInputUseCase useCase = new SubmitInputUseCase(runRepository, serviceRegistry);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .submit(
                        new SubmitInputCommand(
                            new RunId("run_1"), new TenantId("tenant_1"), Map.of("answer", "x")))
                    .toCompletableFuture()
                    .join());

    assertEquals(ErrorCode.INVALID_REQUEST, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals(0, adapter.submitInputCount.get());
  }

  @Test
  void rejectsInputWhenServiceDoesNotSupportHumanInput() {
    serviceRegistry.register(serviceDescriptor("no-input-agent", true, true, false), adapter);
    Run waiting =
        new Run(
            new RunId("run_3"),
            new ConversationId("conv_1"),
            new TenantId("tenant_1"),
            "no-input-agent",
            RunStatus.WAITING_INPUT,
            null,
            0,
            NOW,
            NOW);
    runRepository.create(waiting, null).toCompletableFuture().join();
    SubmitInputUseCase useCase = new SubmitInputUseCase(runRepository, serviceRegistry);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .submit(
                        new SubmitInputCommand(
                            new RunId("run_3"), new TenantId("tenant_1"), Map.of("answer", "x")))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED,
        ((UseCaseException) exception.getCause()).errorCode());
    assertEquals(0, adapter.submitInputCount.get());
  }

  @Test
  void getsServiceCapabilitiesWithTenantVisibility() {
    GetServiceCapabilitiesUseCase useCase = new GetServiceCapabilitiesUseCase(serviceRegistry);

    ServiceDescriptor descriptor =
        useCase.get("sample-agent", new TenantId("tenant_1")).toCompletableFuture().join();

    assertEquals("sample-agent", descriptor.serviceId());
    assertTrue(descriptor.capabilities().streaming());
    assertTrue(descriptor.capabilities().multimodalInput());
  }

  private static void assertInvalidInput(
      SubmitInputUseCase useCase, Map<String, Object> payload, String message) {
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .submit(
                        new SubmitInputCommand(
                            new RunId("run_1"), new TenantId("tenant_1"), payload))
                    .toCompletableFuture()
                    .join());

    assertEquals(ErrorCode.INVALID_REQUEST, ((UseCaseException) exception.getCause()).errorCode());
    assertEquals(message, exception.getCause().getMessage());
  }

  @Test
  void listsOnlyTenantVisibleServicesInStableOrder() {
    serviceRegistry.register(serviceDescriptor("alpha-agent", true, true, true), adapter);
    serviceRegistry.register(
        new ServiceDescriptor(
            "hidden-agent",
            ServiceType.AGENT,
            URI.create("https://internal.example/hidden-agent"),
            "sample-adapter",
            Set.of("tenant_2"),
            Set.of(Transport.SSE),
            new ServiceCapabilities(true, true, false, false, true, true, true, true),
            1),
        adapter);
    ListServicesUseCase useCase = new ListServicesUseCase(serviceRegistry);

    List<ServiceDescriptor> services =
        useCase.list(new TenantId("tenant_1")).toCompletableFuture().join();

    assertEquals(
        List.of("alpha-agent", "sample-agent"),
        services.stream().map(ServiceDescriptor::serviceId).toList());
  }

  @Test
  void rejectsInvisibleTenantWhenGettingServiceCapabilities() {
    GetServiceCapabilitiesUseCase useCase = new GetServiceCapabilitiesUseCase(serviceRegistry);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase.get("sample-agent", new TenantId("tenant_2")).toCompletableFuture().join());

    assertEquals(ErrorCode.FORBIDDEN, ((UseCaseException) exception.getCause()).errorCode());
  }

  @Test
  void rejectsUnknownServiceWhenGettingCapabilities() {
    GetServiceCapabilitiesUseCase useCase = new GetServiceCapabilitiesUseCase(serviceRegistry);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                useCase
                    .get("missing-service", new TenantId("tenant_1"))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        ErrorCode.SERVICE_NOT_FOUND, ((UseCaseException) exception.getCause()).errorCode());
  }

  private CreateRunCommand createCommand(String idempotencyKey) {
    return createCommand("sample-agent", new TenantId("tenant_1"), idempotencyKey);
  }

  private CreateRunUseCase createRunUseCaseWithImageValidation(
      ImageHostResolver imageHostResolver, ImageUrlProbe imageUrlProbe) {
    return new CreateRunUseCase(
        runRepository,
        serviceRegistry,
        eventPublisher,
        new InMemoryRunIdGenerator(),
        clock,
        new NoopRunTelemetry(),
        new NoopRunConcurrencyLimiter(),
        new NoopRunCircuitBreaker(),
        new InputReferenceValidator(imageHostResolver, imageUrlProbe));
  }

  private CreateRunCommand createCommand(
      String serviceId, TenantId tenantId, String idempotencyKey) {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        tenantId,
        serviceId,
        idempotencyKey,
        List.of(new InputPart(InputPartType.TEXT, "hello", null)),
        Map.of());
  }

  private CreateRunCommand createImageCommand(
      String idempotencyKey, String imageUrl, String mimeType) {
    return createImageCommand(
        "sample-agent", new TenantId("tenant_1"), idempotencyKey, imageUrl, mimeType);
  }

  private CreateRunCommand createImageCommand(
      String serviceId,
      TenantId tenantId,
      String idempotencyKey,
      String imageUrl,
      String mimeType) {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        tenantId,
        serviceId,
        idempotencyKey,
        List.of(
            new InputPart(InputPartType.TEXT, "describe this image", null),
            new InputPart(InputPartType.IMAGE_REF, imageUrl, mimeType)),
        Map.of());
  }

  private Run acceptedRun(String runId, String tenantId, RunStatus status, long version) {
    return new Run(
        new RunId(runId),
        new ConversationId("conv_1"),
        new TenantId(tenantId),
        runId.equals("run_2") ? "no-cancel-agent" : "sample-agent",
        status,
        null,
        version,
        NOW,
        NOW);
  }

  private static GatewayEvent event(
      String eventId, EventType eventType, long sequence, Map<String, Object> payload) {
    return GatewayEvent.of(
        eventId,
        eventType,
        "conv_1",
        "run_1",
        "sample-agent",
        sequence,
        NOW,
        payload,
        new EventMetadata("tenant_1", null, Map.of()));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher) {
    CollectingSubscriber subscriber = new CollectingSubscriber();
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  private static byte[] pngPrefix() {
    return new byte[] {(byte) 0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a};
  }

  private CreateRunUseCase createRunUseCaseWithRetryPolicy(CreateRunRetryPolicy retryPolicy) {
    return new CreateRunUseCase(
        runRepository,
        serviceRegistry,
        eventPublisher,
        new InMemoryRunIdGenerator(),
        clock,
        new NoopRunTelemetry(),
        new NoopRunConcurrencyLimiter(),
        new NoopRunCircuitBreaker(),
        new InputReferenceValidator(),
        retryPolicy);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId, boolean streaming, boolean cancel, boolean humanInput) {
    return serviceDescriptor(serviceId, streaming, cancel, humanInput, true);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId,
      boolean streaming,
      boolean cancel,
      boolean humanInput,
      boolean multimodalInput) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(
            streaming, cancel, false, false, true, humanInput, multimodalInput, true),
        new ServiceLimits(500, 100),
        1);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId,
      boolean streaming,
      boolean cancel,
      boolean humanInput,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerTenant) {
    return serviceDescriptor(
        serviceId,
        streaming,
        cancel,
        humanInput,
        Set.of("tenant_1"),
        maxConcurrentRuns,
        maxConcurrentRunsPerTenant);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId,
      boolean streaming,
      boolean cancel,
      boolean humanInput,
      String serviceGroup,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerTenant,
      int maxConcurrentRunsPerGroup) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(streaming, cancel, false, false, true, humanInput, true, true),
        serviceGroup,
        new ServiceLimits(maxConcurrentRuns, maxConcurrentRunsPerTenant, maxConcurrentRunsPerGroup),
        1);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId,
      boolean streaming,
      boolean cancel,
      boolean humanInput,
      String serviceGroup,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerTenant,
      int maxConcurrentRunsPerGroup,
      ServiceCircuitBreakerPolicy circuitBreakerPolicy) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(streaming, cancel, false, false, true, humanInput, true, true),
        serviceGroup,
        new ServiceLimits(maxConcurrentRuns, maxConcurrentRunsPerTenant, maxConcurrentRunsPerGroup),
        circuitBreakerPolicy,
        1);
  }

  private ServiceDescriptor serviceDescriptor(
      String serviceId,
      boolean streaming,
      boolean cancel,
      boolean humanInput,
      Set<String> visibleTenantIds,
      int maxConcurrentRuns,
      int maxConcurrentRunsPerTenant) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        visibleTenantIds,
        Set.of(Transport.SSE),
        new ServiceCapabilities(streaming, cancel, false, false, true, humanInput, true, true),
        new ServiceLimits(maxConcurrentRuns, maxConcurrentRunsPerTenant),
        1);
  }

  private ServiceDescriptor resumableServiceDescriptor(String serviceId) {
    return resumableServiceDescriptor(serviceId, RetrySafety.NONE);
  }

  private ServiceDescriptor resumableServiceDescriptor(String serviceId, RetrySafety retrySafety) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, true, true, true, true, true, true, retrySafety),
        1);
  }

  private ServiceDescriptor serviceDescriptorWithRetrySafety(
      String serviceId, RetrySafety retrySafety) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, true, true, true, retrySafety),
        new ServiceLimits(500, 100),
        1);
  }

  private ServiceDescriptor serviceDescriptorWithAllowedImageHosts(
      String serviceId, Set<String> allowedImageHosts) {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.AGENT,
        URI.create("https://internal.example/" + serviceId),
        "sample-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, true, true, true),
        null,
        ServiceLimits.defaults(),
        ServiceCircuitBreakerPolicy.defaults(),
        allowedImageHosts,
        1);
  }

  private static final class FakeDownstreamExecutionPort implements DownstreamExecutionPort {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger executeFailuresBeforeSuccess = new AtomicInteger();
    private final AtomicInteger cancelCount = new AtomicInteger();
    private final AtomicInteger resumeCount = new AtomicInteger();
    private final AtomicInteger resumeFailuresBeforeSuccess = new AtomicInteger();
    private final AtomicInteger submitInputCount = new AtomicInteger();
    private Map<String, Object> submittedPayload = Map.of();
    private ExecutionHandle submittedHandle;
    private String resumeCursor;
    private ExecutionContext resumeContext;
    private List<GatewayEvent> events =
        List.of(event("evt_1", EventType.EXECUTION_STARTED, 0, Map.of()));
    private List<GatewayEvent> resumeEvents = List.of();
    private Flow.Publisher<GatewayEvent> eventsPublisherOverride;

    @Override
    public CompletionStage<AdapterExecution> execute(
        CreateRunCommand command, ExecutionContext context) {
      executeCount.incrementAndGet();
      if (executeFailuresBeforeSuccess.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
        return CompletableFuture.failedFuture(new IllegalStateException("execute failed"));
      }
      return CompletableFuture.completedFuture(
          new AdapterExecution(
              new ExecutionHandle(
                  command.serviceId(), "sample-adapter", "task_1", null, null, Map.of()),
              eventsPublisherOverride == null
                  ? new StaticGatewayEventPublisher(events)
                  : eventsPublisherOverride));
    }

    @Override
    public CompletionStage<DownstreamExecutionState> cancel(
        ExecutionHandle handle, ExecutionContext context) {
      cancelCount.incrementAndGet();
      return CompletableFuture.completedFuture(DownstreamExecutionState.CANCELED);
    }

    @Override
    public CompletionStage<DownstreamExecutionState> queryState(
        ExecutionHandle handle, ExecutionContext context) {
      return CompletableFuture.completedFuture(DownstreamExecutionState.RUNNING);
    }

    @Override
    public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
        ExecutionHandle handle, String cursor, ExecutionContext context) {
      resumeCount.incrementAndGet();
      if (resumeFailuresBeforeSuccess.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
        return CompletableFuture.failedFuture(new IllegalStateException("resume failed"));
      }
      resumeCursor = cursor;
      resumeContext = context;
      return CompletableFuture.completedFuture(new StaticGatewayEventPublisher(resumeEvents));
    }

    @Override
    public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
      submitInputCount.incrementAndGet();
      submittedPayload = command.payload();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> submitInput(
        SubmitInputCommand command, ExecutionHandle handle, ExecutionContext context) {
      submitInputCount.incrementAndGet();
      submittedPayload = command.payload();
      submittedHandle = handle;
      return CompletableFuture.completedFuture(null);
    }
  }

  private static final class FakeImageUploadCredentialIssuer
      implements ImageUploadCredentialIssuer {

    private final AtomicInteger issueCount = new AtomicInteger();
    private ServiceDescriptor service;
    private String mimeType;
    private long contentLengthBytes;

    @Override
    public CompletionStage<ImageUploadCredential> issue(
        TenantId tenantId,
        ServiceDescriptor service,
        String filename,
        String mimeType,
        long contentLengthBytes) {
      issueCount.incrementAndGet();
      this.service = service;
      this.mimeType = mimeType;
      this.contentLengthBytes = contentLengthBytes;
      return CompletableFuture.completedFuture(
          new ImageUploadCredential(
              "https://upload.example/upload",
              "https://cdn.example.com/object",
              "PUT",
              Map.of("Content-Type", mimeType),
              NOW.plus(Duration.ofMinutes(15)),
              NOW.plus(Duration.ofHours(24)),
              contentLengthBytes));
    }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final List<GatewayEvent> events = new java.util.ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(GatewayEvent item) {
      events.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(List.copyOf(events));
    }
  }
}
