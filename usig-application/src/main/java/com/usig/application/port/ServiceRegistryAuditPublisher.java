package com.usig.application.port;

import com.usig.application.serviceaudit.ServiceRegistryAuditEvent;
import java.util.concurrent.CompletionStage;

public interface ServiceRegistryAuditPublisher {

  CompletionStage<Void> publish(ServiceRegistryAuditEvent event);
}
