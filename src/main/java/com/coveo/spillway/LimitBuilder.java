package com.coveo.spillway;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class LimitBuilder<T> {

  private String limitName;
  private Duration limitExpiration;
  private int limitCapacity;

  private Function<T, String> propertyExtractor;
  private List<LimitTrigger> triggers = new ArrayList<>();

  private LimitBuilder() {}

  public LimitBuilder<T> to(int capacity) {
    this.limitCapacity = capacity;
    return this;
  }

  public LimitBuilder<T> per(Duration expiration) {
    this.limitExpiration = expiration;
    return this;
  }

  public LimitBuilder<T> withLimitTrigger(LimitTrigger limitTrigger) {
    this.triggers.add(limitTrigger);
    return this;
  }

  public LimitBuilder<T> withExceededCallback(LimitTriggerCallback limitTriggerCallback) {
    triggers.add(new ValueThresholdTrigger(limitCapacity, limitTriggerCallback));
    return this;
  }

  public Limit<T> build() {
    return new Limit<>(
        new LimitDefinition(limitName, limitCapacity, limitExpiration),
        propertyExtractor,
        triggers);
  }

  public static LimitBuilder<String> of(String limitName) {
    return of(limitName, Function.identity());
  }

  public static <T> LimitBuilder<T> of(String limitName, Function<T, String> propertyExtractor) {
    LimitBuilder<T> limitBuilder = new LimitBuilder<>();
    limitBuilder.limitName = limitName;
    limitBuilder.propertyExtractor = propertyExtractor;
    return limitBuilder;
  }
}
