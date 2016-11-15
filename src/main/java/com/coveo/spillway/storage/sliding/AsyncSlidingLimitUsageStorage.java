/**
 * The MIT License
 * Copyright (c) 2016 Coveo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.coveo.spillway.storage.sliding;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.SlidingCacheSynchronisation;
import com.google.common.annotations.VisibleForTesting;

/**
 * An asynchronous sliding window implementation of {@link LimitUsageStorage}.
 * <p>
 * This storage internally uses a {@link InMemorySlidingStorage} as cache and performs
 * asynchronous calls to synchronize the counts to a central storage.
 * <p>
 * Since the counts are cached, incoming requests will never be slowed down by problems with
 * the central storage.
 * <p>
 * This storage does not synchronize with the main server each time a request arrives. A configurable
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
  private final LimitUsageStorage wrappedLimitUsageStorage;
  private final InMemorySlidingStorage cache;
  private final Timer timer;

  public AsyncSlidingLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      Duration timeBetweenSynchronisations,
      Duration cacheRetentionTime,
      Duration cacheSlideSize) {
    this(
        wrappedLimitUsageStorage,
        timeBetweenSynchronisations,
        Duration.ofMillis(0),
        cacheRetentionTime,
        cacheSlideSize);
  }

  @VisibleForTesting
  AsyncSlidingLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      Duration timeBetweenSynchronisations,
      Duration delayBeforeFirstSync,
      Duration cacheRetentionTime,
      Duration cacheSlideSize) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.cache = new InMemorySlidingStorage(cacheRetentionTime, cacheSlideSize);

    timer = new Timer();
    timer.schedule(
        new SlidingCacheSynchronisation(
            cache, wrappedLimitUsageStorage, cacheSlideSize.plus(timeBetweenSynchronisations)),
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
