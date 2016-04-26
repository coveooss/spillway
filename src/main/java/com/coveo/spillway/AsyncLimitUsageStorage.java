package com.coveo.spillway;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AsyncLimitUsageStorage implements LimitUsageStorage {

  private final LimitUsageStorage wrappedLimitUsageStorage;
  private final ExecutorService executorService;
  private LimitUsageCache cache;

  public AsyncLimitUsageStorage(LimitUsageStorage wrappedLimitUsageStorage) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.executorService = Executors.newCachedThreadPool();
    this.cache = new LimitUsageCache();
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> cachedEntries = new HashMap<>();
    for (AddAndGetRequest request : requests) {

      Instant expirationDate = request.getBucket().plus(request.getExpiration());
      LimitKey limitEntry = LimitKey.fromRequest(request);

      int cachedValue = cache.get(limitEntry, expirationDate);
      cachedEntries.put(limitEntry, cachedValue);
    }
    executorService.submit(() -> sendAndCacheRequests(requests));

    return cachedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return wrappedLimitUsageStorage.debugCurrentLimitCounters();
  }

  public void sendAndCacheRequests(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> responses = wrappedLimitUsageStorage.addAndGet(requests);

    for (AddAndGetRequest request : requests) {
      // TODO - GSIMARD: Duplicated code above?
      Instant expirationDate = request.getBucket().plus(request.getExpiration());
      LimitKey limitEntry = LimitKey.fromRequest(request);

      cache.add(limitEntry, expirationDate, responses.get(limitEntry));
    }
  }

  // TODO - GISMARD: Rework into InMemoryStorage?
  public class LimitUsageCache {
    private Map<Instant, Map<LimitKey, Integer>> cache = new ConcurrentHashMap<>();

    public void add(LimitKey limitKey, Instant expirationDate, int counter) {
      Map<LimitKey, Integer> cachedEntries =
          cache.computeIfAbsent(expirationDate, (k) -> new HashMap<>());
      cachedEntries.put(limitKey, counter);

      // Delete outdated entries
      Instant now = Instant.now();
      Set<Instant> expiredDates =
          cache
              .keySet()
              .stream()
              .filter(expiration -> now.isAfter(expiration))
              .collect(Collectors.toSet());
      cache.keySet().removeAll(expiredDates);
    }

    public int get(LimitKey limitKey, Instant expirationDate) {
      Map<LimitKey, Integer> map =
          Optional.ofNullable(cache.get(expirationDate)).orElse(new HashMap<>());
      return Optional.ofNullable(map.get(limitKey)).orElse(0);
    }
  }
}
