package com.coveo.spillway.storage.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.TimerTask;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.sliding.AsyncSlidingLimitUsageStorage;
import com.coveo.spillway.storage.sliding.InMemorySlidingStorage;
import com.coveo.spillway.storage.sliding.SlidingLimitUsageStorage;

/**
 * Synchronization {@link TimerTask} that is launched periodically
 * by the {@link AsyncSlidingLimitUsageStorage}.
 *
 * @author Emile Fugulin
 * @since 1.1.0
 */
public class SlidingCacheSynchronisation extends TimerTask {
  private static final Logger logger = LoggerFactory.getLogger(CacheSynchronization.class);

  private InMemorySlidingStorage cache;
  private SlidingLimitUsageStorage storage;
  private Duration durationToSync;

  public SlidingCacheSynchronisation(
      InMemorySlidingStorage cache, SlidingLimitUsageStorage storage, Duration durationToSync) {
    this.cache = cache;
    this.storage = storage;
    this.durationToSync = durationToSync;
  }

  @Override
  public void run() {
    Instant oldestKey = Instant.now().minus(durationToSync);

    cache.applyOnEach(
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
            logger.warn("Exception during synchronization, ignoring.", e);
          }
        });
  }
}
