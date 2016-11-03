package com.coveo.spillway.storage;

import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.CacheSynchronisation;

public class AsyncBatchLimitUsageStorage implements LimitUsageStorage
{
  private final LimitUsageStorage wrappedLimitUsageStorage;
  private InMemoryStorage cache;
  private Timer timer;
  
  public AsyncBatchLimitUsageStorage(LimitUsageStorage wrappedLimitUsageStorage, int timeBetweenSynchronisations) {
    this(wrappedLimitUsageStorage, timeBetweenSynchronisations, 0);
  }

  public AsyncBatchLimitUsageStorage(LimitUsageStorage wrappedLimitUsageStorage, int timeBetweenSynchronisations, int delayBeforeFirstSync) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.cache = new InMemoryStorage();
    
    timer = new Timer();
    timer.scheduleAtFixedRate(new CacheSynchronisation(cache, wrappedLimitUsageStorage), delayBeforeFirstSync, timeBetweenSynchronisations);
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> cachedEntries = cache.addAndGet(requests);

    return cachedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return wrappedLimitUsageStorage.debugCurrentLimitCounters();
  }
  
  public Map<LimitKey, Integer> debugCacheLimitCounters() {
    return cache.debugCurrentLimitCounters();
  }
}