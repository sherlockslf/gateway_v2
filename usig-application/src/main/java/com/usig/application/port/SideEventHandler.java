package com.usig.application.port;

import com.usig.application.sideevent.SideEvent;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface SideEventHandler {

  CompletionStage<Void> handle(SideEvent event);
}
