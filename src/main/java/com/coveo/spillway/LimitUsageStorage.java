package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface LimitUsageStorage {

  default int incrementAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp) {
    return addAndGet(resource, limitName, property, expiration, eventTimestamp, 1);
  }

  default int addAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp,
      int incrementBy) {
    return addAndGet(
        new AddAndGetRequest.Builder()
            .withResource(resource)
            .withLimitName(limitName)
            .withProperty(property)
            .withExpiration(expiration)
            .withEventTimestamp(eventTimestamp)
            .withIncrementBy(incrementBy)
            .build());
  }

  default int addAndGet(AddAndGetRequest request) {
    return addAndGet(Arrays.asList(request)).get(0);
  }

  List<Integer> addAndGet(List<AddAndGetRequest> requests);

  Map<LimitKey, Integer> debugCurrentLimitCounters();
}
