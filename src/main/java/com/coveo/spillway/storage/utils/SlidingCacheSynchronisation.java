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
package com.coveo.spillway.storage.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.TimerTask;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.sliding.AsyncSlidingLimitUsageStorage;
import com.coveo.spillway.storage.sliding.InMemorySlidingStorage;

/**
 * Synchronization {@link TimerTask} that is launched periodically
 * by the {@link AsyncSlidingLimitUsageStorage}.
 *
 * @author Emile Fugulin
 * @since 1.1.0
 */
public class SlidingCacheSynchronisation extends TimerTask {
  private static final Logger logger = LoggerFactory.getLogger(SlidingCacheSynchronisation.class);

  private InMemorySlidingStorage cache;
  private LimitUsageStorage storage;
  private Duration durationToSync;

  public SlidingCacheSynchronisation(
      InMemorySlidingStorage cache, LimitUsageStorage storage, Duration durationToSync) {
    this.cache = cache;
    this.storage = storage;
    this.durationToSync = durationToSync;
  }

  @Override
  public void run() {
    Instant oldestKey = Instant.now().minus(durationToSync);

    cache.applyForEachLimitKey(
        instantEntry -> {
          try {
            Instant bucket = instantEntry.getKey();

            if (bucket.isBefore(oldestKey)) {
              return;
            }

            instantEntry
                .getValue()
                .entrySet()
                .forEach(
                    valueEntry -> {
                      int cost = valueEntry.getValue().getDelta();

                      LimitKey limitKey = valueEntry.getKey();

                      AddAndGetRequest request =
                          new AddAndGetRequest.Builder()
                              .withResource(limitKey.getResource())
                              .withLimitName(limitKey.getLimitName())
                              .withProperty(limitKey.getProperty())
                              .withExpiration(cache.getSlideSize())
                              .withEventTimestamp(limitKey.getBucket())
                              .withCost(cost)
                              .build();

                      Pair<LimitKey, Integer> reponse = storage.addAndGet(request);

                      valueEntry.getValue().substractAndGet(cost);
                      valueEntry.getValue().setTotal(reponse.getValue());
                    });
          } catch (Exception e) {
            logger.error("Exception during synchronization, ignoring.", e);
          }
        });
  }
}
