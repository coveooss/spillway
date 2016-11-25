package com.coveo.spillway.exception;

public class SpillwayLimitWithSameNameException extends SpillwayException {
  private static final long serialVersionUID = -327233779708979738L;

  private static final String ERROR_MESSAGE = "Some limits have the same name";

  public SpillwayLimitWithSameNameException() {
    super(ERROR_MESSAGE);
  }

  public SpillwayLimitWithSameNameException(String message) {
    super(ERROR_MESSAGE + " : " + message);
  }
}
