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
package com.coveo.spillway.storage;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.CacheSynchronization;

/**
 * An asynchronous implementation of {@link LimitUsageStorage}.
 * <p>
 * This storage internally uses a {@link InMemoryStorage} as cache and performs
 * asynchronous calls to the distributed storage to share information.
 * <p>
 * This it particularly useful when using a database over the network as
 * the queries are not slowed down by any external problems.
 * <p>
 * The difference with {@link AsyncLimitUsageStorage} is that this storage
 * does not sync with the main server each time a query arrives. A configurable
 * timeout launches the synchronization.
 * <p>
 * The advantage of this method is that the load on the network and on the external
 * storage is considerably reduced at the cost of a less precise throttling. We
 * recommend to set a relatively small time between each synchronization to avoid
 * big differences between the throttling instances.
 *
 * @author Emile Fugulin
 * @author Simon Toussaint
 * @since 1.0.0
 */
public class AsyncBatchLimitUsageStorage implements LimitUsageStorage {
  private final LimitUsageStorage wrappedLimitUsageStorage;
  private InMemoryStorage cache;
  private Timer timer;

  public AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage, Duration timeBetweenSynchronizations) {
    this(
        wrappedLimitUsageStorage,
        new InMemoryStorage(),
        timeBetweenSynchronizations,
        Duration.ofMillis(0),
        false);
  }

  public AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      Duration timeBetweenSynchronizations,
      boolean forceCacheInit) {
    this(
        wrappedLimitUsageStorage,
        new InMemoryStorage(),
        timeBetweenSynchronizations,
        Duration.ofMillis(0),
        forceCacheInit);
  }

  /*package*/ AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      InMemoryStorage cache,
      Duration timeBetweenSynchronisations,
      Duration delayBeforeFirstSync,
      boolean forceCacheInit) {
    this(
        wrappedLimitUsageStorage,
        cache,
        new CacheSynchronization(cache, wrappedLimitUsageStorage),
        timeBetweenSynchronisations,
        delayBeforeFirstSync,
        forceCacheInit);
  }

  /*package*/ AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      InMemoryStorage cache,
      CacheSynchronization cacheSynchronization,
      Duration timeBetweenSynchronisations,
      Duration delayBeforeFirstSync,
      boolean forceCacheInit) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.cache = cache;

    if (forceCacheInit) {
      cacheSynchronization.init();
    }

    timer = new Timer();
    timer.schedule(
        cacheSynchronization,
        delayBeforeFirstSync.toMillis(),
        timeBetweenSynchronisations.toMillis());
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    return cache.addAndGet(requests);
  }

  public Map<LimitKey, Integer> debugCacheLimitCounters() {
    return cache.getCurrentLimitCounters();
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters() {
    return wrappedLimitUsageStorage.getCurrentLimitCounters();
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(String resource) {
    return wrappedLimitUsageStorage.getCurrentLimitCounters(resource);
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(String resource, String limitName) {
    return wrappedLimitUsageStorage.getCurrentLimitCounters(resource, limitName);
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(
      String resource, String limitName, String property) {
    return wrappedLimitUsageStorage.getCurrentLimitCounters(resource, limitName, property);
  }

  @Override
  public void close() throws Exception {
    timer.cancel();
    wrappedLimitUsageStorage.close();
    cache.close();
  }
}
