package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Optional;

public interface OpenAiCompatibleSecretStore {

  Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context);
}
