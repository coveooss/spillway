package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public interface LimitUsageStorage {

  int incrementAndGetCounter(String resource, String limitName, String property, Duration expiration, Instant eventTimestamp);

  Map<LimitKey, Integer> debugCurrentLimitCounters();
}
