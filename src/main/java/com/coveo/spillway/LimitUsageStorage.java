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

import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public interface LimitUsageStorage {

  default Pair<LimitKey, Integer> incrementAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp) {
    return addAndGet(resource, limitName, property, expiration, eventTimestamp, 1);
  }

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

  default Pair<LimitKey, Integer> addAndGet(AddAndGetRequest request) {
    Map.Entry<LimitKey, Integer> value =
        addAndGet(Arrays.asList(request)).entrySet().iterator().next();
    return Pair.of(value.getKey(), value.getValue());
  }

  Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests);

  Map<LimitKey, Integer> debugCurrentLimitCounters();
}
