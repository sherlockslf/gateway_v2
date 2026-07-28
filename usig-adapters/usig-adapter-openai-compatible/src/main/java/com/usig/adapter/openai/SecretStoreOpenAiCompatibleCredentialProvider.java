package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;
import java.util.Optional;

public final class SecretStoreOpenAiCompatibleCredentialProvider
    implements OpenAiCompatibleCredentialProvider {

  private final OpenAiCompatibleSecretStore secretStore;

  public SecretStoreOpenAiCompatibleCredentialProvider(OpenAiCompatibleSecretStore secretStore) {
    this.secretStore = Objects.requireNonNull(secretStore, "secretStore must not be null");
  }

  @Override
  public Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context) {
    return secretStore.bearerToken(service, context);
  }
}
