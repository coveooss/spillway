package com.coveo.spillway.storage.sliding;

import java.time.Duration;

import com.coveo.spillway.storage.LimitUsageStorage;

public interface SlidingLimitUsageStorage extends LimitUsageStorage {
  Duration getRetention();

  Duration getSlideSize();
}
