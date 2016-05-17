package com.coveo.spillway;

import com.coveo.spillway.memory.InMemoryStorage;
import com.coveo.spillway.memory.OverrideKeyRequest;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AsyncLimitUsageStorage implements LimitUsageStorage {

  private static final Logger logger = LoggerFactory.getLogger(AsyncLimitUsageStorage.class);

  private final LimitUsageStorage wrappedLimitUsageStorage;
  private final ExecutorService executorService;
  private InMemoryStorage cache;

  public AsyncLimitUsageStorage(LimitUsageStorage wrappedLimitUsageStorage) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.executorService = Executors.newSingleThreadExecutor();
    this.cache = new InMemoryStorage();
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> cachedEntries = cache.addAndGet(requests);
    executorService.submit(() -> sendAndCacheRequests(requests));

    return cachedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return wrappedLimitUsageStorage.debugCurrentLimitCounters();
  }

  public void sendAndCacheRequests(Collection<AddAndGetRequest> requests) {
    try {
      Map<LimitKey, Integer> responses = wrappedLimitUsageStorage.addAndGet(requests);

      // Flatten all requests into a single list of overrides.
      Map<Pair<LimitKey, Instant>, Integer> rawOverrides = new HashMap<>();
      for (AddAndGetRequest request : requests) {
        LimitKey limitEntry = LimitKey.fromRequest(request);
        Instant expirationDate = request.getBucket().plus(request.getExpiration());

        rawOverrides.merge(
            Pair.of(limitEntry, expirationDate), responses.get(limitEntry), Integer::sum);
      }
      List<OverrideKeyRequest> overrides =
          rawOverrides
              .entrySet()
              .stream()
              .map(
                  kvp
                      -> new OverrideKeyRequest(
                          kvp.getKey().getLeft(), kvp.getKey().getRight(), kvp.getValue()))
              .collect(Collectors.toList());
      cache.overrideKeys(overrides);
    } catch (RuntimeException ex) {
      logger.warn("Failed to send and cache requests.", ex);
    }
  }
}
