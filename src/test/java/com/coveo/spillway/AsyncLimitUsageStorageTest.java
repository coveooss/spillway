package com.coveo.spillway;

import com.coveo.spillway.memory.InMemoryStorage;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;

public class AsyncLimitUsageStorageTest {

  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Instant INSTANT = Instant.now();
  private static final Duration EXPIRATION = Duration.ofHours(1);

  private AsyncLimitUsageStorage asyncStorage;
  private LimitUsageStorage inMemoryStorage = new InMemoryStorage();
  private AddAndGetRequest request =
      new AddAndGetRequest.Builder()
          .withResource(RESOURCE)
          .withProperty(PROPERTY)
          .withLimitName(LIMITNAME)
          .withEventTimestamp(INSTANT)
          .withCost(1)
          .withExpiration(EXPIRATION)
          .build();

  @Before
  public void setup() {
    asyncStorage = new AsyncLimitUsageStorage(inMemoryStorage);
  }

  @Test
  public void canAddAndGet() throws InterruptedException {
    // Testing async code is hard..
    int counter = asyncStorage.addAndGet(request).getValue();
    assertThat(counter).isEqualTo(0);
    boolean wasIncremented = false;
    for (int i = 0; i < 10 && !wasIncremented; i++) {
      wasIncremented = asyncStorage.debugCurrentLimitCounters().size() > 0;
    }
    assertThat(wasIncremented).isTrue();
  }
}
