package com.usig.application.port;

import com.usig.application.sideevent.SideEvent;
import java.util.concurrent.CompletionStage;

public interface SideEventPublisher {

  CompletionStage<Void> publish(SideEvent event);
}
