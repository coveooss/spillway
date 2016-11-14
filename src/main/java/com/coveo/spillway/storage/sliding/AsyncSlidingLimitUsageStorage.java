package com.coveo.spillway.storage.sliding;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.SlidingCacheSynchronisation;

public class AsyncSlidingLimitUsageStorage implements LimitUsageStorage
{
  private final SlidingLimitUsageStorage wrappedLimitUsageStorage;
  private InMemorySlidingStorage cache;
  private Timer timer;

  public AsyncSlidingLimitUsageStorage(
      SlidingLimitUsageStorage wrappedLimitUsageStorage, Duration timeBetweenSynchronisations) {
    this(wrappedLimitUsageStorage, timeBetweenSynchronisations, Duration.ofMillis(0));
  }

  /*package*/ AsyncSlidingLimitUsageStorage(
      SlidingLimitUsageStorage wrappedLimitUsageStorage,
      Duration timeBetweenSynchronisations,
      Duration delayBeforeFirstSync) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.cache = new InMemorySlidingStorage(wrappedLimitUsageStorage.getRetention(), wrappedLimitUsageStorage.getSlideSize());

    timer = new Timer();
    timer.schedule(
        new SlidingCacheSynchronisation(cache, wrappedLimitUsageStorage, timeBetweenSynchronisations.multipliedBy(2)),
        delayBeforeFirstSync.toMillis(),
        timeBetweenSynchronisations.toMillis());
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    return cache.addAndGet(requests);
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    return wrappedLimitUsageStorage.debugCurrentLimitCounters();
  }

  public Map<LimitKey, Integer> debugCacheLimitCounters() {
    return cache.debugCurrentLimitCounters();
  }

  @Override
  public void close() throws Exception {
    wrappedLimitUsageStorage.close();
  }
}