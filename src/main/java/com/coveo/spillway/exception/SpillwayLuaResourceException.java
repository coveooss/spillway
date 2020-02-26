package com.coveo.spillway.exception;

public class SpillwayLuaResourceException extends RuntimeException {

  public SpillwayLuaResourceException(String message) {
    super(message);
  }

  public SpillwayLuaResourceException(String message, Throwable cause) {
    super("Error reading Resource: " + message, cause);
  }
}
