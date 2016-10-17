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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.limit.LimitUsageStorage;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * An asynchronous implementation of {@link LimitUsageStorage}.
 * </p>
 * This storage internally uses a {@link InMemoryStorage} as cache and performs
 * asynchronous calls to the distributed storage to share information.
 * </p>
 * This it particularly usefull when using a database over the network as
 * the queries are not slowed down by any external problems. 
 * 
 * @author Guillaume Simard
 * @since 1.0.0
 */
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
