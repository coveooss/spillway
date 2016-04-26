package com.coveo.spillway;

public class PercentageThresholdTrigger extends AbstractLimitTrigger {

  private final double triggerPercentage;

  public PercentageThresholdTrigger(double triggerPercentage, LimitTriggerCallback callback) {
    super(callback);

    if (triggerPercentage <= 0 || triggerPercentage >= 1) {
      throw new IllegalArgumentException("Trigger Percentage must be between 0 and 1");
    }

    this.triggerPercentage = triggerPercentage;
  }

  public double getTriggerPercentage() {
    return triggerPercentage;
  }

  @Override
  protected <T> boolean triggered(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition) {
    int previousLimitValue = currentLimitValue - cost;
    double previousPercentage = previousLimitValue / (double) limitDefinition.getCapacity();
    double currentPercentage = currentLimitValue / (double) limitDefinition.getCapacity();

    return currentPercentage > triggerPercentage && previousPercentage <= triggerPercentage;
  }
}
