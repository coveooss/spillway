package com.coveo.spillway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Spillway<T> {

  private static final Logger logger = LoggerFactory.getLogger(Spillway.class);

  private final LimitUsageStorage storage;
  private final String resource;
  private final List<Limit<T>> limits;

  public Spillway(LimitUsageStorage storage, String resourceName, Limit<T>... limits) {
    this.storage = storage;
    this.resource = resourceName;
    this.limits = Arrays.asList(limits);
  }

  public void call(T context) throws SpillwayLimitExceededException {
    call(context, 1);
  }

  public void call(T context, int cost) throws SpillwayLimitExceededException {
    List<LimitDefinition> exceededLimits = getExceededLimits(context, cost);
    if (!exceededLimits.isEmpty()) {
      throw new SpillwayLimitExceededException(exceededLimits, context, cost);
    }
  }

  public boolean tryCall(T context) {
    return tryCall(context, 1);
  }

  public boolean tryCall(T context, int cost) {
    return getExceededLimits(context, cost).isEmpty();
  }

  private List<LimitDefinition> getExceededLimits(T context, int cost) {
    List<LimitDefinition> exceededLimits = new ArrayList<>();
    for (Limit<T> limit : limits) {
      String property = limit.getProperty(context);
      Instant now = Instant.now();

      int limitValue =
          storage.addAndGet(resource, limit.getName(), property, limit.getExpiration(), now, cost);

      if (limitValue > limit.getCapacity()) {
        exceededLimits.add(limit.getDefinition());
        try {
          limit
              .getLimitExceededCallback()
              .orElse(LimitExceededCallback.doNothing())
              .handleExceededLimit(limit.getDefinition(), context);
        } catch (RuntimeException ex) {
          logger.warn(
              "Limit exceeded callback for limit {} threw an exception. Ignoring.", limit, ex);
        }
      }
    }
    return exceededLimits;
  }

  /**
   * This is a costly operation that should only be used for debugging.
   * Limits should always be enforced through the call and tryCall methods.
   * @return
   */
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return storage.debugCurrentLimitCounters();
  }
}
