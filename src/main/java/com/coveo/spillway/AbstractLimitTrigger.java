package com.coveo.spillway;

public abstract class AbstractLimitTrigger implements LimitTrigger {

  private final LimitTriggerCallback callback;

  public AbstractLimitTrigger(LimitTriggerCallback callback) {
    this.callback = callback;
  }

  protected abstract <T> boolean triggered(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition);

  @Override
  public <T> void callbackIfRequired(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition) {
    if (triggered(context, cost, currentLimitValue, limitDefinition)) {
      callback.trigger(limitDefinition, context);
    }
  }
}
