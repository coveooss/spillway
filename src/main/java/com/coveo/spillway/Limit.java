package com.coveo.spillway;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public class Limit<T> {

  private LimitDefinition definition;
  private Function<T, String> propertyExtractor;

  private Optional<LimitExceededCallback> limitExceededCallback;

  /*package*/ Limit(
      LimitDefinition definition,
      Function<T, String> propertyExtractor,
      Optional<LimitExceededCallback> limitExceededCallback) {
    this.definition = definition;
    this.propertyExtractor = propertyExtractor;
    this.limitExceededCallback = limitExceededCallback;
  }

  public LimitDefinition getDefinition() {
    return definition;
  }

  public Optional<LimitExceededCallback> getLimitExceededCallback() {
    return limitExceededCallback;
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
