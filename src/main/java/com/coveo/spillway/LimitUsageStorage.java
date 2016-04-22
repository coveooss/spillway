package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;
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

  int addAndGet(
      String resource,
      String limitName,
      String property,
      Duration expiration,
      Instant eventTimestamp,
      int incrementBy);

  Map<LimitKey, Integer> debugCurrentLimitCounters();
}
