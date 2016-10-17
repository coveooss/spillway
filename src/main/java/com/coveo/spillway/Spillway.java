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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
                        .withCost(cost)
                        .build())
            .collect(Collectors.toList());

    Collection<Integer> results = storage.addAndGet(requests).values();

    List<LimitDefinition> exceededLimits = new ArrayList<>();
    if (results.size() == limits.size()) {
      int i = 0;
      for (Integer result : results) {
        Limit<T> limit = limits.get(i);

        handleTriggers(context, cost, result, limit);

        if (result > limit.getCapacity()) {
          exceededLimits.add(limit.getDefinition());
        }
        i++;
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
      try {
        trigger.callbackIfRequired(context, cost, currentValue, limit.getDefinition());
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
    return storage.debugCurrentLimitCounters();
  }
}
