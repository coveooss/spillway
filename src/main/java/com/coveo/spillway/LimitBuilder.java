package com.coveo.spillway;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public class LimitBuilder<T> {

  private String limitName;
  private Duration limitExpiration;
  private int limitCapacity;

  private Function<T, String> propertyExtractor;
  private Optional<LimitExceededCallback> limitExceededCallback = Optional.empty();

  private LimitBuilder() {}

  public LimitBuilder<T> to(int capacity) {
    this.limitCapacity = capacity;
    return this;
  }

  public LimitBuilder<T> per(Duration expiration) {
    this.limitExpiration = expiration;
    return this;
  }

  public LimitBuilder<T> withExceededCallback(LimitExceededCallback limitExceededCallback) {
    this.limitExceededCallback = Optional.of(limitExceededCallback);
    return this;
  }

  public Limit<T> build() {
    return new Limit<>(
        new LimitDefinition(limitName, limitCapacity, limitExpiration),
        propertyExtractor,
        limitExceededCallback);
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
