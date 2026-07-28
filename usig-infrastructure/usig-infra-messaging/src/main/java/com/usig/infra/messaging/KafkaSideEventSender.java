package com.usig.infra.messaging;

import java.util.concurrent.CompletionStage;

public interface KafkaSideEventSender {

  CompletionStage<Void> send(KafkaSideEventRecord record);
}
