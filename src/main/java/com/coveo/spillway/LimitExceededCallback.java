package com.coveo.spillway;

@FunctionalInterface
public interface LimitExceededCallback {
  LimitExceededCallback DO_NOTHING = (limitDefinition, context) -> {};

  void handleExceededLimit(LimitDefinition definition, Object context);

  static LimitExceededCallback doNothing() {
    return DO_NOTHING;
  }
}
