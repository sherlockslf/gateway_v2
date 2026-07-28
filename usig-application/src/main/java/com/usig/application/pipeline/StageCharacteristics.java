package com.usig.application.pipeline;

public record StageCharacteristics(
    boolean synchronous,
    boolean possiblyBlocking,
    boolean preservesOrder,
    boolean discardable,
    boolean accessesDatabase) {

  public static StageCharacteristics synchronousNonBlockingOrdered() {
    return new StageCharacteristics(true, false, true, false, false);
  }
}
