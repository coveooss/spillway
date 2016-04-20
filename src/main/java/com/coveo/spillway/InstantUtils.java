package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;

public class InstantUtils {
  private InstantUtils() {
  }

  public static Instant truncate(Instant instant, Duration truncationDuration) {
    return Instant.ofEpochMilli((instant.toEpochMilli() / truncationDuration.toMillis()) * truncationDuration.toMillis());
  }
}
