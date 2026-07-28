package com.usig.domain.run;

public final class InvalidRunTransitionException extends RuntimeException {

  public InvalidRunTransitionException(RunStatus current, RunStatus next) {
    super("Invalid run status transition: " + current + " -> " + next);
  }
}
