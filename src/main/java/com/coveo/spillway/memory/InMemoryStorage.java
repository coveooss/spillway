package com.coveo.spillway.memory;

import com.coveo.spillway.AddAndGetRequest;
import com.coveo.spillway.LimitKey;
import com.coveo.spillway.LimitUsageStorage;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InMemoryStorage implements LimitUsageStorage {

  Map<Instant, Map<LimitKey, AtomicInteger>> map = new ConcurrentHashMap<>();
  private Object lock = new Object();

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> updatedEntries = new HashMap<>();
    synchronized (lock) {
      for (AddAndGetRequest request : requests) {
        Instant expirationDate = request.getBucket().plus(request.getExpiration());

        LimitKey limitKey = LimitKey.fromRequest(request);

        Map<LimitKey, AtomicInteger> mapWithThisExpiration =
                map.computeIfAbsent(expirationDate, (key) -> new HashMap<>());
        AtomicInteger counter =
                mapWithThisExpiration.computeIfAbsent(limitKey, (key) -> new AtomicInteger(0));
        updatedEntries.put(limitKey, counter.addAndGet(request.getCost()));
      }
      removeExpiredEntries();
    }
    return updatedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    removeExpiredEntries();
    return map.values()
            .stream()
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, kvp -> kvp.getValue().get()));
  }

  public void overrideKeys(List<OverrideKeyRequest> overrides) {
    synchronized (lock) {
      for (OverrideKeyRequest override : overrides) {
        map.computeIfAbsent(override.getExpirationDate(), d -> new HashMap<>()).put(override.getLimitKey(), new AtomicInteger(override.getNewValue()));
      }
    }
  }

  private void removeExpiredEntries() {
    Instant now = Instant.now();
    Set<Instant> expiredDates =
            map.keySet()
                    .stream()
                    .filter(expiration -> now.isAfter(expiration))
                    .collect(Collectors.toSet());
    map.keySet().removeAll(expiredDates);
  }
}
