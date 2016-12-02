package com.coveo.spillway.limit.utils;

import java.time.Duration;
import java.time.Instant;

public class LimitUtils
{
  public static Instant calculateBucket(Instant timestamp, Duration limitDuration) {
    return Instant.ofEpochMilli(
        (timestamp.toEpochMilli() / limitDuration.toMillis()) * limitDuration.toMillis());
  }
}