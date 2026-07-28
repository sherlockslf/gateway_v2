package com.usig.application.sideevent;

public record SideEventOutboxDrainResult(int attempted, int published, int failed) {

  public SideEventOutboxDrainResult {
    if (attempted < 0 || published < 0 || failed < 0) {
      throw new IllegalArgumentException("drain counters must not be negative");
    }
    if (published + failed != attempted) {
      throw new IllegalArgumentException("published + failed must equal attempted");
    }
  }

  public static SideEventOutboxDrainResult empty() {
    return new SideEventOutboxDrainResult(0, 0, 0);
  }
}
