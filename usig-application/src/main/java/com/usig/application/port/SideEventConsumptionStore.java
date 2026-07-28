package com.usig.application.port;

import com.usig.application.sideevent.SideEventConsumptionClaim;
import java.util.concurrent.CompletionStage;

public interface SideEventConsumptionStore {

  CompletionStage<SideEventConsumptionClaim> claim(String eventId);

  CompletionStage<Void> markConsumed(String eventId);

  CompletionStage<Void> release(String eventId);
}
