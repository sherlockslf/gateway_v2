package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Optional;

public interface OpenAiCompatibleCredentialProvider {

  Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context);
}
