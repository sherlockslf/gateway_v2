package com.usig.bootstrap;

import com.usig.adapter.sample.SampleAgentAdapter;
import com.usig.adapter.spi.StreamingServiceAdapter;
import com.usig.application.memory.InMemoryImageUploadCredentialIssuer;
import com.usig.application.memory.InMemoryReplayStore;
import com.usig.application.memory.InMemoryRunCircuitBreaker;
import com.usig.application.memory.InMemoryRunConcurrencyLimiter;
import com.usig.application.memory.InMemoryRunIdGenerator;
import com.usig.application.memory.InMemoryRunRepository;
import com.usig.application.memory.ReliabilityOptions;
import com.usig.application.memory.ReliableRunEventPublisher;
import com.usig.application.port.ClockPort;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ImageUploadCompletionVerifier;
import com.usig.application.port.ImageUploadCredentialIssuer;
import com.usig.application.port.ImageUploadLifecycleCleaner;
import com.usig.application.port.RunCircuitBreaker;
import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.application.port.RunEventPublisher;
import com.usig.application.port.RunIdGenerator;
import com.usig.application.port.RunRepository;
import com.usig.application.port.RunTelemetry;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.port.ServiceRegistryReloader;
import com.usig.application.port.ServiceRegistrySnapshotSource;
import com.usig.application.serviceaudit.InMemoryServiceRegistryAuditPublisher;
import com.usig.application.serviceaudit.ServiceRegistrySideEventAuditPublisher;
import com.usig.application.sideevent.InMemorySideEventOutbox;
import com.usig.application.sideevent.InMemorySideEventPublisher;
import com.usig.application.sideevent.SideEventDispatchPolicy;
import com.usig.application.sideevent.SideEventDispatcher;
import com.usig.application.sideevent.SideEventOutboxDrainPolicy;
import com.usig.application.sideevent.SideEventOutboxDrainer;
import com.usig.application.usecase.CancelRunUseCase;
import com.usig.application.usecase.ConfirmImageUploadUseCase;
import com.usig.application.usecase.CreateRunRetryPolicy;
import com.usig.application.usecase.CreateRunUseCase;
import com.usig.application.usecase.GetRunUseCase;
import com.usig.application.usecase.GetServiceCapabilitiesUseCase;
import com.usig.application.usecase.GetServiceRegistryReloadStatusUseCase;
import com.usig.application.usecase.ListServicesUseCase;
import com.usig.application.usecase.ReloadServiceRegistryFromSourceUseCase;
import com.usig.application.usecase.ReloadServiceRegistryUseCase;
import com.usig.application.usecase.RequestImageUploadCredentialUseCase;
import com.usig.application.usecase.ResumeRetryPolicy;
import com.usig.application.usecase.ServiceRegistryReloadPolicy;
import com.usig.application.usecase.SubmitInputUseCase;
import com.usig.application.usecase.SubscribeRunEventsUseCase;
import com.usig.infra.objectstorage.S3CompatibleImageUploadCredentialIssuer;
import com.usig.infra.objectstorage.S3CompatibleObjectStorageConfig;
import com.usig.infra.observability.OpenTelemetryRunTelemetry;
import com.usig.infra.observability.QuarkusOpenTelemetrySink;
import com.usig.infra.postgres.PostgresServiceRegistry;
import com.usig.infra.postgres.PostgresServiceRegistryCommands;
import com.usig.infra.postgres.PostgresServiceRegistrySnapshotSource;
import com.usig.infra.redis.RedisCircuitBreakerCommands;
import com.usig.infra.redis.RedisConcurrencyCommands;
import com.usig.infra.redis.RedisRunCircuitBreaker;
import com.usig.infra.redis.RedisRunConcurrencyLimiter;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.Config;

@ApplicationScoped
public final class UsigRuntimeConfiguration {

  private final ClockPort clock = Instant::now;
  private final InMemoryRunRepository runRepository = new InMemoryRunRepository(clock);
  private final ReliableRunEventPublisher runEventPublisher =
      new ReliableRunEventPublisher(
          new InMemoryReplayStore(256, 8 * 1024 * 1024), ReliabilityOptions.defaults());
  private final InMemoryRunIdGenerator runIdGenerator = new InMemoryRunIdGenerator();
  private final ImageUploadCredentialIssuer imageUploadCredentialIssuer;
  private final ImageUploadCompletionVerifier imageUploadCompletionVerifier;
  private final ImageUploadLifecycleCleaner imageUploadLifecycleCleaner;
  private final InMemoryServiceRegistryAuditPublisher serviceRegistryAuditPublisher =
      new InMemoryServiceRegistryAuditPublisher(256);
  private final InMemorySideEventPublisher sideEventPublisher = new InMemorySideEventPublisher(256);
  private final InMemorySideEventOutbox sideEventOutbox;
  private final ServiceRegistry serviceRegistry;
  private final ServiceRegistryReloader serviceRegistryReloader;
  private final ServiceRegistryReloadPolicy serviceRegistryReloadPolicy;
  private final ReloadServiceRegistryFromSourceUseCase reloadServiceRegistryFromSourceUseCase;
  private final ServiceRegistryReloadWorkerConfig serviceRegistryReloadWorkerConfig;
  private final ServiceRegistryReloadWorker serviceRegistryReloadWorker;
  private final Map<String, DownstreamExecutionPort> adaptersById;
  private final SideEventDispatchPolicy sideEventDispatchPolicy;
  private final SideEventDispatcher sideEventDispatcher;
  private final SideEventOutboxDrainPolicy sideEventOutboxDrainPolicy;
  private final SideEventOutboxDrainer sideEventOutboxDrainer;
  private final SideEventOutboxDrainWorkerConfig sideEventOutboxDrainWorkerConfig;
  private final SideEventOutboxDrainWorker sideEventOutboxDrainWorker;
  private final RunConcurrencyLimiter runConcurrencyLimiter;
  private final RunCircuitBreaker runCircuitBreaker;
  private final CreateRunRetryPolicy createRunRetryPolicy;
  private final ResumeRetryPolicy resumeRetryPolicy;

  @Inject
  UsigRuntimeConfiguration(
      Config config,
      RedisConcurrencyCommands redisConcurrencyCommands,
      RedisCircuitBreakerCommands redisCircuitBreakerCommands,
      Instance<PostgresServiceRegistryCommands> postgresServiceRegistryCommands) {
    SampleAgentAdapter sampleAgentAdapter = new SampleAgentAdapter();
    Map<String, StreamingServiceAdapter> streamingAdaptersById =
        Map.of(sampleAgentAdapter.adapterId(), sampleAgentAdapter);
    adaptersById = Map.copyOf(streamingAdaptersById);
    sideEventDispatchPolicy = sideEventDispatchPolicy(config);
    sideEventOutbox = sideEventOutbox(config);
    sideEventOutboxDrainPolicy = sideEventOutboxDrainPolicy(config);
    sideEventDispatcher =
        new SideEventDispatcher(
            sideEventPublisher, sideEventDispatchPolicy, sideEventOutbox, Clock.systemUTC());
    serviceRegistry =
        serviceRegistry(
            config,
            streamingAdaptersById,
            new ServiceRegistrySideEventAuditPublisher(
                serviceRegistryAuditPublisher, sideEventDispatcher),
            postgresServiceRegistryCommands,
            clock);
    serviceRegistryReloader = serviceRegistryReloader(serviceRegistry);
    serviceRegistryReloadPolicy = serviceRegistryReloadPolicy(config);
    reloadServiceRegistryFromSourceUseCase =
        new ReloadServiceRegistryFromSourceUseCase(
            serviceRegistrySnapshotSource(config, adaptersById, postgresServiceRegistryCommands),
            serviceRegistryReloader);
    serviceRegistryReloadWorkerConfig = serviceRegistryReloadWorkerConfig(config);
    serviceRegistryReloadWorker =
        new ServiceRegistryReloadWorker(
            reloadServiceRegistryFromSourceUseCase, serviceRegistryReloadWorkerConfig);
    serviceRegistryReloadWorker.start();
    sideEventOutboxDrainer =
        new SideEventOutboxDrainer(
            sideEventOutbox, sideEventPublisher, sideEventOutboxDrainPolicy, Clock.systemUTC());
    sideEventOutboxDrainWorkerConfig = sideEventOutboxDrainWorkerConfig(config);
    sideEventOutboxDrainWorker =
        new SideEventOutboxDrainWorker(sideEventOutboxDrainer, sideEventOutboxDrainWorkerConfig);
    sideEventOutboxDrainWorker.start();
    runConcurrencyLimiter = runConcurrencyLimiter(config, redisConcurrencyCommands);
    runCircuitBreaker = runCircuitBreaker(config, redisCircuitBreakerCommands, clock);
    createRunRetryPolicy = createRunRetryPolicy(config);
    resumeRetryPolicy = resumeRetryPolicy(config);
    ImageUploadPorts imageUploadPorts = imageUploadPorts(config, clock);
    imageUploadCredentialIssuer = imageUploadPorts.credentialIssuer();
    imageUploadCompletionVerifier = imageUploadPorts.completionVerifier();
    imageUploadLifecycleCleaner = imageUploadPorts.lifecycleCleaner();
  }

  @Produces
  @Singleton
  ClockPort clockPort() {
    return clock;
  }

  @Produces
  @Singleton
  RunRepository runRepository() {
    return runRepository;
  }

  @Produces
  @Singleton
  RunEventPublisher runEventPublisher() {
    return runEventPublisher;
  }

  @Produces
  @Singleton
  RunIdGenerator runIdGenerator() {
    return runIdGenerator;
  }

  @Produces
  @Singleton
  ServiceRegistry serviceRegistryBean() {
    return serviceRegistry;
  }

  @Produces
  @Singleton
  InMemoryServiceRegistryAuditPublisher inMemoryServiceRegistryAuditPublisher() {
    return serviceRegistryAuditPublisher;
  }

  @Produces
  @Singleton
  InMemorySideEventPublisher inMemorySideEventPublisher() {
    return sideEventPublisher;
  }

  @Produces
  @Singleton
  InMemorySideEventOutbox inMemorySideEventOutbox() {
    return sideEventOutbox;
  }

  @Produces
  @Singleton
  SideEventDispatchPolicy sideEventDispatchPolicy() {
    return sideEventDispatchPolicy;
  }

  @Produces
  @Singleton
  SideEventDispatcher sideEventDispatcher() {
    return sideEventDispatcher;
  }

  @Produces
  @Singleton
  SideEventOutboxDrainPolicy sideEventOutboxDrainPolicy() {
    return sideEventOutboxDrainPolicy;
  }

  @Produces
  @Singleton
  SideEventOutboxDrainer sideEventOutboxDrainer() {
    return sideEventOutboxDrainer;
  }

  @Produces
  @Singleton
  SideEventOutboxDrainWorkerConfig sideEventOutboxDrainWorkerConfig() {
    return sideEventOutboxDrainWorkerConfig;
  }

  @Produces
  @Singleton
  SideEventOutboxDrainWorker sideEventOutboxDrainWorker() {
    return sideEventOutboxDrainWorker;
  }

  @Produces
  @Singleton
  RunConcurrencyLimiter runConcurrencyLimiter() {
    return runConcurrencyLimiter;
  }

  @Produces
  @Singleton
  RunCircuitBreaker runCircuitBreaker() {
    return runCircuitBreaker;
  }

  @Produces
  @Singleton
  CreateRunRetryPolicy createRunRetryPolicy() {
    return createRunRetryPolicy;
  }

  @Produces
  @Singleton
  ResumeRetryPolicy resumeRetryPolicy() {
    return resumeRetryPolicy;
  }

  @Produces
  @Singleton
  RunTelemetry runTelemetry(OpenTelemetry openTelemetry) {
    return new OpenTelemetryRunTelemetry(new QuarkusOpenTelemetrySink(openTelemetry));
  }

  @Produces
  @Singleton
  ImageUploadCredentialIssuer imageUploadCredentialIssuer() {
    return imageUploadCredentialIssuer;
  }

  @Produces
  @Singleton
  ImageUploadCompletionVerifier imageUploadCompletionVerifier() {
    return imageUploadCompletionVerifier;
  }

  @Produces
  @Singleton
  ImageUploadLifecycleCleaner imageUploadLifecycleCleaner() {
    return imageUploadLifecycleCleaner;
  }

  @Produces
  @Singleton
  CreateRunUseCase createRunUseCase(RunTelemetry runTelemetry) {
    return new CreateRunUseCase(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        runConcurrencyLimiter,
        runCircuitBreaker,
        createRunRetryPolicy);
  }

  @Produces
  @Singleton
  GetRunUseCase getRunUseCase() {
    return new GetRunUseCase(runRepository);
  }

  @Produces
  @Singleton
  CancelRunUseCase cancelRunUseCase() {
    return new CancelRunUseCase(runRepository, serviceRegistry);
  }

  @Produces
  @Singleton
  SubscribeRunEventsUseCase subscribeRunEventsUseCase() {
    return new SubscribeRunEventsUseCase(
        runRepository, runEventPublisher, serviceRegistry, resumeRetryPolicy);
  }

  @Produces
  @Singleton
  SubmitInputUseCase submitInputUseCase() {
    return new SubmitInputUseCase(runRepository, serviceRegistry);
  }

  @Produces
  @Singleton
  GetServiceCapabilitiesUseCase getServiceCapabilitiesUseCase() {
    return new GetServiceCapabilitiesUseCase(serviceRegistry);
  }

  @Produces
  @Singleton
  ListServicesUseCase listServicesUseCase() {
    return new ListServicesUseCase(serviceRegistry);
  }

  @Produces
  @Singleton
  ReloadServiceRegistryUseCase reloadServiceRegistryUseCase() {
    return new ReloadServiceRegistryUseCase(
        serviceRegistryReloader, adaptersById, serviceRegistryReloadPolicy);
  }

  @Produces
  @Singleton
  GetServiceRegistryReloadStatusUseCase getServiceRegistryReloadStatusUseCase() {
    return new GetServiceRegistryReloadStatusUseCase(
        serviceRegistryReloadWorker, serviceRegistryReloadPolicy);
  }

  @Produces
  @Singleton
  ReloadServiceRegistryFromSourceUseCase reloadServiceRegistryFromSourceUseCase() {
    return reloadServiceRegistryFromSourceUseCase;
  }

  @Produces
  @Singleton
  ServiceRegistryReloadWorkerConfig serviceRegistryReloadWorkerConfig() {
    return serviceRegistryReloadWorkerConfig;
  }

  @Produces
  @Singleton
  ServiceRegistryReloadWorker serviceRegistryReloadWorker() {
    return serviceRegistryReloadWorker;
  }

  @Produces
  @Singleton
  RequestImageUploadCredentialUseCase requestImageUploadCredentialUseCase(
      ImageUploadCredentialIssuer credentialIssuer) {
    return new RequestImageUploadCredentialUseCase(serviceRegistry, credentialIssuer);
  }

  @Produces
  @Singleton
  ConfirmImageUploadUseCase confirmImageUploadUseCase(
      ImageUploadCompletionVerifier completionVerifier) {
    return new ConfirmImageUploadUseCase(serviceRegistry, completionVerifier);
  }

  private static RunConcurrencyLimiter runConcurrencyLimiter(
      Config config, RedisConcurrencyCommands redisConcurrencyCommands) {
    String backend =
        config.getOptionalValue("usig.concurrency.backend", String.class).orElse("memory");
    if ("memory".equalsIgnoreCase(backend)) {
      return new InMemoryRunConcurrencyLimiter();
    }
    if ("redis".equalsIgnoreCase(backend)) {
      String keyPrefix =
          config.getOptionalValue("usig.redis.key-prefix", String.class).orElse("usig");
      return new RedisRunConcurrencyLimiter(redisConcurrencyCommands, keyPrefix);
    }
    throw new IllegalArgumentException("unsupported usig.concurrency.backend: " + backend);
  }

  private static ServiceRegistry serviceRegistry(
      Config config,
      Map<String, StreamingServiceAdapter> adaptersById,
      ServiceRegistrySideEventAuditPublisher auditPublisher,
      Instance<PostgresServiceRegistryCommands> postgresServiceRegistryCommands,
      ClockPort clock) {
    String backend =
        config.getOptionalValue("usig.service-registry.backend", String.class).orElse("memory");
    if ("memory".equalsIgnoreCase(backend)) {
      return ConfiguredServiceRegistryFactory.create(
          config, adaptersById, auditPublisher, clock::now);
    }
    if ("postgres".equalsIgnoreCase(backend)) {
      if (postgresServiceRegistryCommands.isUnsatisfied()) {
        throw new IllegalStateException("PostgresServiceRegistryCommands bean not configured");
      }
      return new PostgresServiceRegistry(
          postgresServiceRegistryCommands.get(),
          adaptersById.entrySet().stream()
              .collect(
                  java.util.stream.Collectors.toUnmodifiableMap(
                      Map.Entry::getKey, Map.Entry::getValue)));
    }
    throw new IllegalArgumentException("unsupported usig.service-registry.backend: " + backend);
  }

  private static SideEventDispatchPolicy sideEventDispatchPolicy(Config config) {
    Set<String> failFastAuditTenants =
        csv(
            config
                .getOptionalValue("usig.side-events.fail-fast-audit-tenants", String.class)
                .orElse(""));
    return SideEventDispatchPolicy.failFastAuditForTenants(failFastAuditTenants);
  }

  private static ServiceRegistryReloader serviceRegistryReloader(ServiceRegistry serviceRegistry) {
    if (serviceRegistry instanceof ServiceRegistryReloader reloader) {
      return reloader;
    }
    return entries -> {
      throw new IllegalStateException("service registry reload is not supported by active backend");
    };
  }

  private static ServiceRegistryReloadPolicy serviceRegistryReloadPolicy(Config config) {
    return new ServiceRegistryReloadPolicy(
        config
            .getOptionalValue("usig.service-registry.reload.enabled", Boolean.class)
            .orElse(false),
        config
            .getOptionalValue("usig.service-registry.reload.admin-token", String.class)
            .orElse(""));
  }

  private static ServiceRegistrySnapshotSource serviceRegistrySnapshotSource(
      Config config,
      Map<String, DownstreamExecutionPort> adaptersById,
      Instance<PostgresServiceRegistryCommands> postgresServiceRegistryCommands) {
    String source =
        config.getOptionalValue("usig.service-registry.reload.source", String.class).orElse("none");
    if ("none".equalsIgnoreCase(source)) {
      return () ->
          java.util.concurrent.CompletableFuture.failedFuture(
              new IllegalStateException("service registry reload source is not configured"));
    }
    if ("postgres".equalsIgnoreCase(source)) {
      if (postgresServiceRegistryCommands.isUnsatisfied()) {
        throw new IllegalStateException("PostgresServiceRegistryCommands bean not configured");
      }
      return new PostgresServiceRegistrySnapshotSource(
          postgresServiceRegistryCommands.get(), adaptersById);
    }
    throw new IllegalArgumentException(
        "unsupported usig.service-registry.reload.source: " + source);
  }

  private static ServiceRegistryReloadWorkerConfig serviceRegistryReloadWorkerConfig(
      Config config) {
    ServiceRegistryReloadWorkerConfig defaults = ServiceRegistryReloadWorkerConfig.defaults();
    return new ServiceRegistryReloadWorkerConfig(
        config
            .getOptionalValue("usig.service-registry.reload.worker.enabled", Boolean.class)
            .orElse(defaults.enabled()),
        config
            .getOptionalValue("usig.service-registry.reload.worker.interval", Duration.class)
            .orElse(defaults.interval()));
  }

  private static InMemorySideEventOutbox sideEventOutbox(Config config) {
    return new InMemorySideEventOutbox(
        config.getOptionalValue("usig.side-events.outbox.max-entries", Integer.class).orElse(512));
  }

  private static SideEventOutboxDrainPolicy sideEventOutboxDrainPolicy(Config config) {
    SideEventOutboxDrainPolicy defaults = SideEventOutboxDrainPolicy.defaults();
    return new SideEventOutboxDrainPolicy(
        config
            .getOptionalValue("usig.side-events.outbox.drain.max-batch-size", Integer.class)
            .orElse(defaults.maxBatchSize()),
        config
            .getOptionalValue("usig.side-events.outbox.drain.retry-delay", Duration.class)
            .orElse(defaults.retryDelay()));
  }

  private static SideEventOutboxDrainWorkerConfig sideEventOutboxDrainWorkerConfig(Config config) {
    SideEventOutboxDrainWorkerConfig defaults = SideEventOutboxDrainWorkerConfig.defaults();
    return new SideEventOutboxDrainWorkerConfig(
        config
            .getOptionalValue("usig.side-events.outbox.drain.worker.enabled", Boolean.class)
            .orElse(defaults.enabled()),
        config
            .getOptionalValue("usig.side-events.outbox.drain.worker.interval", Duration.class)
            .orElse(defaults.interval()));
  }

  private static Set<String> csv(String value) {
    if (value == null || value.isBlank()) {
      return Set.of();
    }
    return java.util.Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(item -> !item.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  private static RunCircuitBreaker runCircuitBreaker(
      Config config, RedisCircuitBreakerCommands redisCircuitBreakerCommands, ClockPort clock) {
    String backend =
        config.getOptionalValue("usig.circuit-breaker.backend", String.class).orElse("memory");
    if ("memory".equalsIgnoreCase(backend)) {
      return new InMemoryRunCircuitBreaker(clock);
    }
    if ("redis".equalsIgnoreCase(backend)) {
      String keyPrefix =
          config.getOptionalValue("usig.redis.key-prefix", String.class).orElse("usig");
      return new RedisRunCircuitBreaker(redisCircuitBreakerCommands, clock, keyPrefix);
    }
    throw new IllegalArgumentException("unsupported usig.circuit-breaker.backend: " + backend);
  }

  private static CreateRunRetryPolicy createRunRetryPolicy(Config config) {
    CreateRunRetryPolicy defaults = CreateRunRetryPolicy.defaults();
    return new CreateRunRetryPolicy(
        config
            .getOptionalValue("usig.retry.create.safe-max-attempts", Integer.class)
            .orElse(defaults.safeCreateMaxAttempts()),
        config
            .getOptionalValue("usig.retry.create.base-delay", Duration.class)
            .orElse(defaults.baseDelay()),
        config
            .getOptionalValue("usig.retry.create.jitter-range", Duration.class)
            .orElse(defaults.jitterRange()));
  }

  private static ResumeRetryPolicy resumeRetryPolicy(Config config) {
    ResumeRetryPolicy defaults = ResumeRetryPolicy.defaults();
    return new ResumeRetryPolicy(
        config
            .getOptionalValue("usig.retry.resume.max-attempts", Integer.class)
            .orElse(defaults.maxAttempts()),
        config
            .getOptionalValue("usig.retry.resume.base-delay", Duration.class)
            .orElse(defaults.baseDelay()),
        config
            .getOptionalValue("usig.retry.resume.jitter-range", Duration.class)
            .orElse(defaults.jitterRange()));
  }

  private static ImageUploadPorts imageUploadPorts(Config config, ClockPort clock) {
    String backend =
        config.getOptionalValue("usig.image-upload.backend", String.class).orElse("memory");
    Duration retentionDuration =
        config
            .getOptionalValue("usig.image-upload.retention-duration", Duration.class)
            .orElse(Duration.ofHours(24));
    if ("memory".equalsIgnoreCase(backend)) {
      InMemoryImageUploadCredentialIssuer issuer =
          new InMemoryImageUploadCredentialIssuer(clock, retentionDuration);
      return new ImageUploadPorts(issuer, issuer, issuer);
    }
    if ("s3-compatible".equalsIgnoreCase(backend)) {
      S3CompatibleImageUploadCredentialIssuer issuer =
          new S3CompatibleImageUploadCredentialIssuer(
              new S3CompatibleObjectStorageConfig(
                  config.getValue("usig.image-upload.s3.endpoint", URI.class),
                  config
                      .getOptionalValue("usig.image-upload.s3.public-base-uri", URI.class)
                      .orElse(null),
                  config.getValue("usig.image-upload.s3.bucket", String.class),
                  config.getValue("usig.image-upload.s3.region", String.class),
                  config.getValue("usig.image-upload.s3.access-key-id", String.class),
                  config.getValue("usig.image-upload.s3.secret-access-key", String.class),
                  config
                      .getOptionalValue("usig.image-upload.s3.session-token", String.class)
                      .orElse(null),
                  config
                      .getOptionalValue(
                          "usig.image-upload.s3.callback-signing-secret", String.class)
                      .orElse(null),
                  config
                      .getOptionalValue("usig.image-upload.s3.upload-ttl", Duration.class)
                      .orElse(Duration.ofMinutes(15)),
                  retentionDuration),
              clock);
      return new ImageUploadPorts(issuer, issuer, issuer);
    }
    throw new IllegalArgumentException("unsupported usig.image-upload.backend: " + backend);
  }

  private record ImageUploadPorts(
      ImageUploadCredentialIssuer credentialIssuer,
      ImageUploadCompletionVerifier completionVerifier,
      ImageUploadLifecycleCleaner lifecycleCleaner) {}

  @PreDestroy
  void stopWorkers() {
    serviceRegistryReloadWorker.stop();
    sideEventOutboxDrainWorker.stop();
  }
}
