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
package com.coveo.spillway.storage;

import org.apache.commons.lang3.tuple.Pair;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Interface that defines a distributed storage that could be used with Spillway.
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public interface LimitUsageStorage {

  /**
   * Increments the specified limit by one and returns the current count.
   *
   * @param resource The resource name on which the limit is enforced
   * @param limitName The name of the limit
   * @param property The name of the property used in the limit
   * @param expiration The duration of the limit before it is reset
   * @param eventTimestamp The Instant at which the event was recorded
   * @return A Pair of the limit and its current count
   */
  default Pair<LimitKey, Integer> incrementAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp) {
    return addAndGet(resource, limitName, property, expiration, eventTimestamp, 1);
  }

  /**
   * Increments the specified limit by the cost and returns the current count.
   *
   * @param resource The resource name on which the limit is enforced
   * @param limitName The name of the limit
   * @param property The name of the property used in the limit
   * @param expiration The duration of the limit before it is reset
   * @param eventTimestamp The Instant at which the event was recorded
   * @param cost The cost the query
   * @return A Pair of the limit and its current count
   */
  default Pair<LimitKey, Integer> addAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp,
      int cost) {
    return addAndGet(
        new AddAndGetRequest.Builder()
            .withResource(resource)
            .withLimitName(limitName)
            .withProperty(property)
            .withExpiration(expiration)
            .withEventTimestamp(eventTimestamp)
            .withCost(cost)
            .build());
  }

  /**
   * Increments the specified limit by the cost and returns the current count.
   *
   * @param request An {@link AddAndGetRequest} that wraps all necessary information to perform the increment
   * @return A Pair of the limit and its current count
   */
  default Pair<LimitKey, Integer> addAndGet(AddAndGetRequest request) {
    Map.Entry<LimitKey, Integer> value =
        addAndGet(Arrays.asList(request)).entrySet().iterator().next();
    return Pair.of(value.getKey(), value.getValue());
  }

  /**
   * Processes all {@link AddAndGetRequest} and returns the current count for each limit.
   *
   * @param requests An collection of {@link AddAndGetRequest} that wrap all necessary information to perform the increments
   * @return A Map of the limits and their current count
   */
  Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests);

  /**
   * @return A Map of all the enforced limits and their current count
   */
  Map<LimitKey, Integer> debugCurrentLimitCounters();

  /**
   * @return A Map of all the enforced limits for a resource and their current count
   */
  Map<LimitKey, Integer> getCurrentLimitCounters(String resource);

  /**
   * @return A Map of a specific limit for a resource and its current count
   */
  Map<LimitKey, Integer> getCurrentLimitCounters(String resource, String limitKey);

  /**
   * @return A Map of a specific limit for a resource and its current count
   */
  Map<LimitKey, Integer> getCurrentLimitCounters(String resource, String limitKey, String property);

  /**
   * Call this method to close the storage when done with it.
   * This method is NOT idempotent.
   *
   * @throws Exception Can throw if unable to close the storage
   */
  void close() throws Exception;
}
