package com.coveo.spillway.storage.utils;

import java.util.TimerTask;

import com.coveo.spillway.storage.InMemoryStorage;
import com.coveo.spillway.storage.LimitUsageStorage;

public class CacheSynchronisation extends TimerTask
{
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
    cache.applyOnEach((entry) -> {
      if (entry != null) {

      }
    });

  }
}