package com.coveo.spillway;

public abstract class SpillwayException extends Exception {

  public SpillwayException(String message) {
    super(message);
  }

  public SpillwayException(String message, Throwable cause) {
    super(message, cause);
  }
}
