package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;
import java.util.Optional;

public final class KmsDecryptingOpenAiCompatibleSecretStore implements OpenAiCompatibleSecretStore {

  private final OpenAiCompatibleSecretStore encryptedSecretStore;
  private final OpenAiCompatibleKmsDecryptor decryptor;

  public KmsDecryptingOpenAiCompatibleSecretStore(
      OpenAiCompatibleSecretStore encryptedSecretStore, OpenAiCompatibleKmsDecryptor decryptor) {
    this.encryptedSecretStore =
        Objects.requireNonNull(encryptedSecretStore, "encryptedSecretStore must not be null");
    this.decryptor = Objects.requireNonNull(decryptor, "decryptor must not be null");
  }

  @Override
  public Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context) {
    return encryptedSecretStore
        .bearerToken(service, context)
        .flatMap(ciphertext -> decryptor.decrypt(ciphertext, service, context))
        .filter(token -> !token.isBlank());
  }
}
