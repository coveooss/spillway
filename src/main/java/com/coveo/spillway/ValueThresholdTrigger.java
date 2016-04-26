package com.coveo.spillway;

public class ValueThresholdTrigger extends AbstractLimitTrigger {
  private int triggerValue;

  public ValueThresholdTrigger(int triggerValue, LimitTriggerCallback callback) {
    super(callback);
    this.triggerValue = triggerValue;
  }

  public int getTriggerValue() {
    return triggerValue;
  }

  @Override
  protected <T> boolean triggered(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition) {
    // Detect if the limit was exceeded by this call() invocation
    // This can be detected if the new value is higher than the limit and the previous value is lower
    // This is possible since the storage guarantees atomicity of operations
    int previousLimitValue = currentLimitValue - cost;
    return currentLimitValue > triggerValue && previousLimitValue <= triggerValue;
  }

  @Override
  public String toString() {
    return "LimitTrigger{" + "limitValue=" + triggerValue + '}';
  }
}
