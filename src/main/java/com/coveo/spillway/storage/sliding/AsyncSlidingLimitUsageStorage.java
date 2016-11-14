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
