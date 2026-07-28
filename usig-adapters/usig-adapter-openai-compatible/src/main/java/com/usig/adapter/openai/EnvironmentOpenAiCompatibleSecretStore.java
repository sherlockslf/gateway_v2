package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public final class EnvironmentOpenAiCompatibleSecretStore implements OpenAiCompatibleSecretStore {

  private static final String DEFAULT_PREFIX = "USIG_OPENAI_TOKEN";

  private final String prefix;
  private final Function<String, String> environment;

  public EnvironmentOpenAiCompatibleSecretStore() {
    this(DEFAULT_PREFIX, System::getenv);
  }

  public EnvironmentOpenAiCompatibleSecretStore(Function<String, String> environment) {
    this(DEFAULT_PREFIX, environment);
  }

  public EnvironmentOpenAiCompatibleSecretStore(
      String prefix, Function<String, String> environment) {
    this.prefix = normalizePrefix(prefix);
    this.environment = Objects.requireNonNull(environment, "environment must not be null");
  }

  @Override
  public Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context) {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(context, "context must not be null");
    String serviceKey = envKey(service.serviceId());
    String tenantKey = envKey(context.tenantId().value());
    return firstPresent(prefix + "_" + tenantKey + "_" + serviceKey)
        .or(() -> firstPresent(prefix + "_" + serviceKey));
  }

  private Optional<String> firstPresent(String key) {
    String value = environment.apply(key);
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  private static String normalizePrefix(String prefix) {
    if (prefix == null || prefix.isBlank()) {
      return DEFAULT_PREFIX;
    }
    return envKey(prefix);
  }

  private static String envKey(String value) {
    StringBuilder normalized = new StringBuilder();
    for (int index = 0; index < value.length(); index++) {
      char current = Character.toUpperCase(value.charAt(index));
      if ((current >= 'A' && current <= 'Z') || (current >= '0' && current <= '9')) {
        normalized.append(current);
      } else {
        normalized.append('_');
      }
    }
    return normalized.toString().replaceAll("_+", "_").replaceAll("^_|_$", "");
  }
}
