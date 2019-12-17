package com.coveo.spillway.exception;

public class SpillwayLuaResourceNotFoundException extends RuntimeException {

  public SpillwayLuaResourceNotFoundException(String message) {
    super(message);
  }

  public SpillwayLuaResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
