package com.coveo.spillway;

@FunctionalInterface
public interface LimitTriggerCallback {
  LimitTriggerCallback DO_NOTHING = (limitDefinition, context) -> {};

  void trigger(LimitDefinition definition, Object context);

  static LimitTriggerCallback doNothing() {
    return DO_NOTHING;
  }
}
