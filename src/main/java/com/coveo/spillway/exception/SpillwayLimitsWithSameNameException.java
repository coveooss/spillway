package com.coveo.spillway.exception;

public class SpillwayLimitsWithSameNameException extends IllegalArgumentException {
  private static final long serialVersionUID = -327233779708979738L;

  private static final String ERROR_MESSAGE = "Some limits have the same name";

  public SpillwayLimitsWithSameNameException() {
    super(ERROR_MESSAGE);
  }

  public SpillwayLimitsWithSameNameException(String message) {
    super(ERROR_MESSAGE + " : " + message);
  }
}
