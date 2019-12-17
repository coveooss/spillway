/**
 * The MIT License
 * Copyright (c) 2016 Coveo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.coveo.spillway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.exception.SpillwayLimitExceededException;
import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.limit.LimitDefinition;
import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.trigger.LimitTrigger;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Contains methods to easily interact with the defined limits in the storage
 * and test if the incoming query should be throttled.
 * <p>
 * Should always be built using the {@link SpillwayFactory}.
 *
 * @param <T> The type of the context. String if not using a propertyExtractor
 *            ({@link LimitBuilder#of(String, java.util.function.Function)}).
 *
 * @author Guillaume Simard
 * @author Emile Fugulin
 * @since 1.0.0
 */
public class Spillway<T> {

  private static final Logger logger = LoggerFactory.getLogger(Spillway.class);

  private final Clock clock;

  private final LimitUsageStorage storage;
  private final String resource;
  private final List<Limit<T>> limits;

  @SafeVarargs
  public Spillway(Clock clock, LimitUsageStorage storage, String resourceName, Limit<T>... limits) {
    this.clock = clock;
    this.storage = storage;
    this.resource = resourceName;
    this.limits = Collections.unmodifiableList(Arrays.asList(limits));
  }

  /**
   * Behaves like {@link #call(Object, int)} with {@code cost} of one.
   *
   * @see #call(Object, int)
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @throws SpillwayLimitExceededException If one the enforced limits is exceeded
   */
  public void call(T context) throws SpillwayLimitExceededException {
    call(context, 1);
  }

  /**
   * Verifies if the query should be throttled using the specified cost.
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @throws SpillwayLimitExceededException If one the enforced limits is exceeded
   */
  public void updateAndVerifyLimit(T context) throws SpillwayLimitExceededException {
    updateAndVerifyLimit(context, 1);
  }

  /**
   * Verifies if the query should be throttled using the specified cost.
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the query, must be greater than zero
   * @throws SpillwayLimitExceededException If one the enforced limits is exceeded
   */
  public void call(T context, int cost) throws SpillwayLimitExceededException {
    List<LimitDefinition> exceededLimits = getExceededLimits(context, cost, true);
    if (!exceededLimits.isEmpty()) {
      throw new SpillwayLimitExceededException(exceededLimits, context, cost);
    }
  }

  /**
   * Verifies if the query should be throttled using the specified cost.
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the query, must be greater than zero
   * @throws SpillwayLimitExceededException If one the enforced limits is exceeded
   */
  public void updateAndVerifyLimit(T context, int cost) throws SpillwayLimitExceededException {
    List<LimitDefinition> exceededLimits = updateAndVerifyExceededLimits(context, cost);
    if (!exceededLimits.isEmpty()) {
      throw new SpillwayLimitExceededException(exceededLimits, context, cost);
    }
  }

  /**
   * Behaves like {@link #tryCall(Object, int)} with {@code cost} of one.
   *
   * @see #tryCall(Object, int)
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @return False if one the enforced limits is exceeded, true otherwise
   */
  public boolean tryCall(T context) {
    return tryCall(context, 1);
  }

  /**
   * Verifies if the query should be throttled using the specified cost.
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the query, greater than zero
   * @return False if one the enforced limits is exceeded, true otherwise
   */
  public boolean tryCall(T context, int cost) {
    return getExceededLimits(context, cost, true).isEmpty();
  }

  /**
   * Behaves like {@link #tryUpdateAndVerifyLimit(Object, int)} with {@code cost} of one.
   *
   * @see #tryUpdateAndVerifyLimit(Object, int)
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @return False if one the enforced limits is exceeded, true otherwise
   */
  public boolean tryUpdateAndVerifyLimit(T context) {
    return tryUpdateAndVerifyLimit(context, 1);
  }

  /**
   * Verifies if the query should be throttled using the specified cost. (This API assures updating and verifying the limit will be a thread safe operation)
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the query, greater than zero
   * @return False if one the enforced limits is exceeded, true otherwise
   */
  public boolean tryUpdateAndVerifyLimit(T context, int cost) {
    return updateAndVerifyExceededLimits(context, cost).isEmpty();
  }

  /**
   * Behaves like {@link #checkLimit(Object, int)} with {@code cost} of one.
   *
   * @see #checkLimit(Object, int)
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @return False if one the enforced limits would be exceeded, true otherwise
   */
  public boolean checkLimit(T context) {
    return checkLimit(context, 1);
  }

  /**
   * Verifies if the given limit will be exceeded for the given cost without updating the limit storage
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the query, must be greater than zero
   * @return False if one the enforced limits would be exceeded, true otherwise
   */
  public boolean checkLimit(T context, int cost) {
    return getExceededLimits(context, cost, false).isEmpty();
  }

  private List<LimitDefinition> getExceededLimits(T context, int cost, boolean shouldUpdateLimit) {
    if (cost < 1) {
      throw new IllegalArgumentException("'cost' must be greater than zero");
    }

    Instant now = Instant.now(clock);
    List<AddAndGetRequest> requests = buildRequestsFromLimits(context, 0, now);

    Map<LimitKey, Integer> results = storage.addAndGet(requests);

    List<LimitDefinition> exceededLimits = new ArrayList<>();
    if (results.size() == limits.size()) {
      for (Entry<LimitKey, Integer> result : results.entrySet()) {
        Limit<T> limit =
            limits
                .stream()
                .filter(entry -> entry.getName().equals(result.getKey().getLimitName()))
                .findFirst()
                .get();

        if (shouldUpdateLimit) {
          handleTriggers(context, cost, now, result.getValue() + cost, limit);
        }

        if (result.getValue() + cost > limit.getCapacity(context)) {
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

    if (shouldUpdateLimit && exceededLimits.isEmpty()) {
      requests = buildRequestsFromLimits(context, cost, now);
      storage.addAndGet(requests);
    }

    return exceededLimits;
  }

  private List<LimitDefinition> updateAndVerifyExceededLimits(T context, int cost) {
    if (cost < 1) {
      throw new IllegalArgumentException("'cost' must be greater than zero");
    }

    Instant now = Instant.now(clock);
    List<AddAndGetRequest> requests = buildRequestsFromLimits(context, cost, now);

    Map<LimitKey, Integer> results = storage.addAndGetWithLimit(requests);

    List<LimitDefinition> exceededLimits = new ArrayList<>();
    if (results.size() == limits.size()) {
      for (Entry<LimitKey, Integer> result : results.entrySet()) {
        Limit<T> limit =
            limits
                .stream()
                .filter(entry -> entry.getName().equals(result.getKey().getLimitName()))
                .findFirst()
                .get();
        int counterValue = result.getValue() + cost;
        handleTriggers(context, cost, now, counterValue, limit);
        if (counterValue > limit.getCapacity(context)) {
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

  private List<AddAndGetRequest> buildRequestsFromLimits(T context, int cost, Instant now) {
    // The existing API respect the minimum limit of all the limits
    // Here, we select the minimum limit
    int minLimit =
        limits.stream().map(limit -> limit.getCapacity(context)).min(Integer::compareTo).orElse(0);
    return limits
        .stream()
        .map(
            limit
                -> new AddAndGetRequest.Builder()
                    .withResource(resource)
                    .withLimitName(limit.getName())
                    .withLimit(minLimit)
                    .withProperty(limit.getProperty(context))
                    .withDistributed(limit.isDistributed())
                    .withExpiration(limit.getExpiration(context))
                    .withEventTimestamp(now)
                    .withCost(cost)
                    .build())
        .collect(Collectors.toList());
  }

  private void handleTriggers(
      T context, int cost, Instant timestamp, int currentValue, Limit<T> limit) {
    for (LimitTrigger trigger : limit.getLimitTriggers(context)) {
      try {
        trigger.callbackIfRequired(
            context, cost, timestamp, currentValue, limit.getDefinition(context));
      } catch (RuntimeException ex) {
        logger.warn(
            "Trigger callback {} for limit {} threw an exception. Ignoring.", trigger, limit, ex);
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
    return storage.getCurrentLimitCounters();
  }
}
