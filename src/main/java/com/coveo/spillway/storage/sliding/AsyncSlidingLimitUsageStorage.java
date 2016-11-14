package com.coveo.spillway.storage.sliding;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.SlidingCacheSynchronisation;

/**
 * An asynchronous sliding window implementation of {@link LimitUsageStorage}.
 * <p>
 * This storage internally uses a {@link InMemorySlidingStorage} as cache and performs
 * asynchronous calls to the distributed storage to share information.
 * <p>
 * This it particularly useful when using a database over the network as
 * the queries are not slowed down by any external problems.
 * <p>
 * This storage does not sync with the main server each time a query arrives. A configurable
 * timeout launches the synchronization.
 * <p>
 * The advantage of this method is that the load on the network and on the external
 * storage is considerably reduced at the cost of a less precise throttling. We
 * recommend to set a relatively small time between each synchronization to avoid
 * big differences between the throttling instances.
 *
 * @author Emile Fugulin
 * @since 1.1.0
 */
public class AsyncSlidingLimitUsageStorage implements LimitUsageStorage {
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
    this.cache =
        new InMemorySlidingStorage(
            wrappedLimitUsageStorage.getRetention(), wrappedLimitUsageStorage.getSlideSize());

    timer = new Timer();
    timer.schedule(
        new SlidingCacheSynchronisation(
            cache,
            wrappedLimitUsageStorage,
            wrappedLimitUsageStorage.getSlideSize().plus(timeBetweenSynchronisations)),
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
