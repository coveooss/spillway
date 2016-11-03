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

import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.CacheSynchronisation;

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
 * @since 1.0.0
 */
public class AsyncBatchLimitUsageStorage implements LimitUsageStorage {
  private final LimitUsageStorage wrappedLimitUsageStorage;
  private InMemoryStorage cache;
  private Timer timer;

  public AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage, int timeBetweenSynchronisations) {
    this(wrappedLimitUsageStorage, timeBetweenSynchronisations, 0);
  }

  public AsyncBatchLimitUsageStorage(
      LimitUsageStorage wrappedLimitUsageStorage,
      int timeBetweenSynchronisations,
      int delayBeforeFirstSync) {
    this.wrappedLimitUsageStorage = wrappedLimitUsageStorage;
    this.cache = new InMemoryStorage();

    timer = new Timer();
    timer.scheduleAtFixedRate(
        new CacheSynchronisation(cache, wrappedLimitUsageStorage),
        delayBeforeFirstSync,
        timeBetweenSynchronisations);
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
