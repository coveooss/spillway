package com.coveo.spillway;

public class SpillwayFactory {
  private final LimitUsageStorage storage;

  public SpillwayFactory(LimitUsageStorage storage)
  {
    this.storage = storage;
  }

  public <T> Spillway<T> enforce(String resource, Limit<T>... limits)
  {
    return new Spillway<>(storage, resource, limits);
  }
}
