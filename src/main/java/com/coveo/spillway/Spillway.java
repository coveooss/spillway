package com.coveo.spillway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Spillway<T> {

  private static final Logger logger = LoggerFactory.getLogger(Spillway.class);

  private final LimitUsageStorage storage;
  private final String resource;
  private final List<Limit<T>> limits;

  @SafeVarargs
  public Spillway(LimitUsageStorage storage, String resourceName, Limit<T>... limits) {
    this.storage = storage;
    this.resource = resourceName;
    this.limits = Collections.unmodifiableList(Arrays.asList(limits));
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
    Instant now = Instant.now();
    List<AddAndGetRequest> requests =
            limits
                    .stream()
                    .map(
                            limit
                                    -> new AddAndGetRequest.Builder()
                                    .withResource(resource)
                                    .withLimitName(limit.getName())
                                    .withProperty(limit.getProperty(context))
                                    .withExpiration(limit.getExpiration())
                                    .withEventTimestamp(now)
                                    .withIncrementBy(cost)
                                    .build())
                    .collect(Collectors.toList());

    List<Integer> results = storage.addAndGet(requests);

    List<LimitDefinition> exceededLimits = new ArrayList<>();
    if (results.size() == limits.size()) {
      for (int i = 0; i < results.size(); i++) {
        int currentValue = results.get(i);
        Limit<T> limit = limits.get(i);

        handleTriggers(context, cost, currentValue, limit);

        if (currentValue > limit.getCapacity()) {
          exceededLimits.add(limit.getDefinition());
        }
      }
    } else {
      logger.error(
              "Something went very wrong. We sent {} limits to the backend but received {} responses. Assuming that no limits were exceeded. Limits: {}. Results: {}.",
              limits.size(),
              results.size(),
              limits,
              results);
    }

    return exceededLimits;
  }

  private void handleTriggers(T context, int cost, int currentValue, Limit<T> limit) {
    for (LimitTrigger trigger : limit.getLimitTriggers()) {
      // Detect if the limit was exceeded by this call() invocation
      // This can be detected if the new value is higher than the limit and the previous value is lower
      // This is possible since the storage guarantees atomicity of operations
      if (currentValue > trigger.getLimitValue()
              && currentValue - cost <= trigger.getLimitValue()) {
        try {
          trigger.getCallback().trigger(limit.getDefinition(), context);
        } catch (RuntimeException ex) {
          logger.warn("Trigger callback {} for limit {} threw an exception. Ignoring.", trigger, limit, ex);
        }
      }
    }
  }

  /**
   * This is a costly operation that should only be used for debugging.
   * Limits should always be enforced through the call and tryCall methods.
   *
   * @return Every limit and its current associated counter.
   */
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return storage.debugCurrentLimitCounters();
  }
}
