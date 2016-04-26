package com.coveo.spillway;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InMemoryStorage implements LimitUsageStorage {

  Map<LimitKey, AtomicInteger> map = new ConcurrentHashMap<>();
  private Object lock = new Object();

  @Override
  public List<Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    List<Integer> counters = new ArrayList<>();
    synchronized (lock) {
      for (AddAndGetRequest request : requests) {
        Instant bucketInstant =
            InstantUtils.truncate(request.getEventTimestamp(), request.getExpiration());

        AtomicInteger newCounter = new AtomicInteger(0);
        AtomicInteger counter =
            map.putIfAbsent(
                new LimitKey(
                    request.getResource(),
                    request.getLimitName(),
                    request.getProperty(),
                    bucketInstant),
                newCounter);
        if (counter == null) {
          // This is the first time we see this key. We need to increment the new counter
          // we just inserted.
          counter = newCounter;
        }
        counters.add(counter.addAndGet(request.getIncrementBy()));
      }
    }
    return counters;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return map.entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get()));
  }
}
