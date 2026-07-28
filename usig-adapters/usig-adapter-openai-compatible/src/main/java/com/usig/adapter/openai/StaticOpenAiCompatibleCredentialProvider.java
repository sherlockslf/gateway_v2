package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class StaticOpenAiCompatibleCredentialProvider
    implements OpenAiCompatibleCredentialProvider {

  private final Map<String, String> tokensByServiceId;

  public StaticOpenAiCompatibleCredentialProvider(Map<String, String> tokensByServiceId) {
    this.tokensByServiceId = Map.copyOf(Objects.requireNonNull(tokensByServiceId));
  }

  @Override
  public Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context) {
    Objects.requireNonNull(service, "service must not be null");
    return Optional.ofNullable(tokensByServiceId.get(service.serviceId()))
        .filter(token -> !token.isBlank());
  }
}
