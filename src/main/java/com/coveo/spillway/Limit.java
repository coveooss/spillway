package com.coveo.spillway;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public class Limit<T> {

  private LimitDefinition definition;
  private Function<T, String> propertyExtractor;

  private List<LimitTrigger> limitTriggers;

  /*package*/ Limit(
      LimitDefinition definition,
      Function<T, String> propertyExtractor,
      List<LimitTrigger> limitTriggers) {
    this.definition = definition;
    this.propertyExtractor = propertyExtractor;
    this.limitTriggers = limitTriggers;
  }

  public LimitDefinition getDefinition() {
    return definition;
  }

  public List<LimitTrigger> getLimitTriggers() {
    return limitTriggers;
  }

  public String getProperty(T context) {
    return propertyExtractor.apply(context);
  }

  public String getName() {
    return definition.getName();
  }

  public Duration getExpiration() {
    return definition.getExpiration();
  }

  public int getCapacity() {
    return definition.getCapacity();
  }

  @Override
  public String toString() {
    return definition.toString();
  }
}
