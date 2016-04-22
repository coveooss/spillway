package com.coveo.spillway;

import java.time.Duration;

public class LimitDefinition {
  private String name;
  private int capacity;
  private Duration expiration;

  public LimitDefinition(String name, int capacity, Duration expiration) {
    this.name = name;
    this.capacity = capacity;
    this.expiration = expiration;
  }

  public String getName() {
    return name;
  }

  public int getCapacity() {
    return capacity;
  }

  public Duration getExpiration() {
    return expiration;
  }

  @Override
  public String toString() {
    return name + "[" + capacity + " calls/" + expiration + "]";
  }
}
