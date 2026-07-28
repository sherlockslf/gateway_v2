package com.usig.adapter.openai;

import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Optional;

public interface OpenAiCompatibleKmsDecryptor {

  Optional<String> decrypt(String ciphertext, ServiceDescriptor service, ExecutionContext context);
}
