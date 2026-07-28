package com.usig.adapter.openai;

import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.Objects;

public record OpenAiCompatibleAdapterConfig(
    ServiceDescriptor service,
    int maxFrameBytes,
    Duration openTimeout,
    Duration firstEventTimeout,
    Duration idleEventTimeout,
    Duration totalExecutionTimeout,
    Duration cancelTimeout,
    String defaultModel,
    String authHeaderName,
    String authToken,
    OpenAiCompatibleApiMode apiMode,
    String cancelEndpointTemplate,
    String queryStateEndpointTemplate,
    String submitInputEndpointTemplate,
    String resumeEndpointTemplate) {

  private static final Duration DEFAULT_TOTAL_EXECUTION_TIMEOUT = Duration.ofHours(24);
  private static final Duration DEFAULT_CANCEL_TIMEOUT = Duration.ofSeconds(30);

  public OpenAiCompatibleAdapterConfig {
    service = Objects.requireNonNull(service, "service must not be null");
    if (maxFrameBytes < 1024) {
      throw new IllegalArgumentException("maxFrameBytes must be at least 1024");
    }
    openTimeout = Objects.requireNonNull(openTimeout, "openTimeout must not be null");
    if (openTimeout.isZero() || openTimeout.isNegative()) {
      throw new IllegalArgumentException("openTimeout must be positive");
    }
    firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    if (firstEventTimeout.isZero() || firstEventTimeout.isNegative()) {
      throw new IllegalArgumentException("firstEventTimeout must be positive");
    }
    idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    if (idleEventTimeout.isZero() || idleEventTimeout.isNegative()) {
      throw new IllegalArgumentException("idleEventTimeout must be positive");
    }
    totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
    if (totalExecutionTimeout.isZero() || totalExecutionTimeout.isNegative()) {
      throw new IllegalArgumentException("totalExecutionTimeout must be positive");
    }
    cancelTimeout = Objects.requireNonNull(cancelTimeout, "cancelTimeout must not be null");
    if (cancelTimeout.isZero() || cancelTimeout.isNegative()) {
      throw new IllegalArgumentException("cancelTimeout must be positive");
    }
    defaultModel = requireText(defaultModel, "defaultModel");
    authHeaderName = normalize(authHeaderName);
    authToken = normalize(authToken);
    if (authToken != null && authHeaderName == null) {
      throw new IllegalArgumentException("authHeaderName must be configured when authToken is set");
    }
    apiMode = Objects.requireNonNullElse(apiMode, OpenAiCompatibleApiMode.CHAT_COMPLETIONS);
    cancelEndpointTemplate = normalize(cancelEndpointTemplate);
    if (cancelEndpointTemplate != null && !cancelEndpointTemplate.contains("{downstreamRunId}")) {
      throw new IllegalArgumentException("cancelEndpointTemplate must contain {downstreamRunId}");
    }
    queryStateEndpointTemplate = normalize(queryStateEndpointTemplate);
    if (queryStateEndpointTemplate != null
        && !queryStateEndpointTemplate.contains("{downstreamRunId}")) {
      throw new IllegalArgumentException(
          "queryStateEndpointTemplate must contain {downstreamRunId}");
    }
    submitInputEndpointTemplate = normalize(submitInputEndpointTemplate);
    if (submitInputEndpointTemplate != null
        && !submitInputEndpointTemplate.contains("{downstreamRunId}")) {
      throw new IllegalArgumentException(
          "submitInputEndpointTemplate must contain {downstreamRunId}");
    }
    resumeEndpointTemplate = normalize(resumeEndpointTemplate);
    if (resumeEndpointTemplate != null && !resumeEndpointTemplate.contains("{downstreamRunId}")) {
      throw new IllegalArgumentException("resumeEndpointTemplate must contain {downstreamRunId}");
    }
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        null,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate,
      String submitInputEndpointTemplate) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        submitInputEndpointTemplate,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate,
      String submitInputEndpointTemplate,
      String resumeEndpointTemplate) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        submitInputEndpointTemplate,
        resumeEndpointTemplate);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        null,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode,
      String cancelEndpointTemplate) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        cancelEndpointTemplate,
        null,
        null,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken,
      OpenAiCompatibleApiMode apiMode) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        apiMode,
        null,
        null,
        null,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service,
      int maxFrameBytes,
      Duration openTimeout,
      String defaultModel,
      String authHeaderName,
      String authToken) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        defaultModel,
        authHeaderName,
        authToken,
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        null);
  }

  public OpenAiCompatibleAdapterConfig(
      ServiceDescriptor service, int maxFrameBytes, Duration openTimeout) {
    this(
        service,
        maxFrameBytes,
        openTimeout,
        openTimeout,
        openTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        "gpt-compatible",
        null,
        null,
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        null);
  }

  private static String requireText(String value, String name) {
    String normalized = normalize(value);
    if (normalized == null) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return normalized;
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
