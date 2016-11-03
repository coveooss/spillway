package com.coveo.spillway.storage.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.TimerTask;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.InMemoryStorage;
import com.coveo.spillway.storage.LimitUsageStorage;

public class CacheSynchronisation extends TimerTask
{
  private static final Logger logger = LoggerFactory.getLogger(CacheSynchronisation.class);

  private InMemoryStorage cache;
  private LimitUsageStorage storage;

  public CacheSynchronisation(InMemoryStorage cache,
                              LimitUsageStorage storage)
  {
    this.cache = cache;
    this.storage = storage;
  }

  @Override
  public void run()
  {
    cache.applyOnEach((instantEntry) -> {
      try {
        Instant expiration = instantEntry.getKey();

        instantEntry.getValue().entrySet().forEach(valueEntry -> {
          int cost = valueEntry.getValue().getDelta();

          LimitKey limitKey = valueEntry.getKey();

          Duration duration = Duration.ofMillis(expiration.toEpochMilli() - limitKey.getBucket().toEpochMilli());

          AddAndGetRequest request = new AddAndGetRequest.Builder().withResource(limitKey.getResource())
                                                                   .withLimitName(limitKey.getLimitName())
                                                                   .withProperty(limitKey.getProperty())
                                                                   .withExpiration(duration)
                                                                   .withEventTimestamp(limitKey.getBucket())
                                                                   .withCost(cost)
                                                                   .build();
          
          Pair<LimitKey, Integer> reponse = storage.addAndGet(request);
          
          valueEntry.getValue().addAndGet(-cost);
          valueEntry.getValue().setTotal(reponse.getValue());
        });
      } catch (IllegalStateException e) {
        logger.warn("Key was deleted during synchronisation, ignoring");
      }
    });

  }
}