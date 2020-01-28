package com.coveo.spillway.exception;

public class SpillwayLuaResourceErrorException extends RuntimeException {

  public SpillwayLuaResourceErrorException(String message) {
    super(message);
  }

  public SpillwayLuaResourceErrorException(String message, Throwable cause) {
    super("Error reading Resource: " + message, cause);
  }
}
