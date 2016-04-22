package com.coveo.spillway;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;

public class InMemoryStorageTest {

  private static final String RESOURCE1 = "someResource";
  private static final String LIMIT1 = "someLimit";
  private static final String PROPERTY1 = "someProperty";
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final Instant TIMESTAMP = Instant.now();

  private InMemoryStorage storage;

  @Before
  public void setup() {
    storage = new InMemoryStorage();
  }

  @Test
  public void canAddLargeValues() {
    int result = storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5);
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void canAddLargeValuesToExisitingCounters() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    int result = storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5);

    assertThat(result).isEqualTo(6);
  }
}
