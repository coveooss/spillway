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
