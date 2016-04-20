package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InMemoryStorage implements LimitUsageStorage {

  Map<LimitKey, AtomicInteger> map = new ConcurrentHashMap<>();
  private Object lock = new Object();

  @Override
  public int incrementAndGetCounter(String resource, String limitName, String property, Duration expiration, Instant eventTimestamp) {
    synchronized (lock) {
      Instant bucketInstant = InstantUtils.truncate(eventTimestamp, expiration);

      AtomicInteger newCounter = new AtomicInteger(0);
      AtomicInteger counter = map.putIfAbsent(new LimitKey(resource, limitName, property, bucketInstant), newCounter);
      if (counter == null) {
        // This is the first time we see this key. We need to increment the new counter
        // we just inserted.
        counter = newCounter;
      }
      return counter.incrementAndGet();
    }
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return map.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get()));
  }
}
