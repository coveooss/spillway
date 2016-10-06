package com.coveo.spillway;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncLimitUsageStorageTest {

  private static final Logger logger = LoggerFactory.getLogger(AsyncLimitUsageStorageTest.class);

  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Instant INSTANT = Instant.now();
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final int MOCKED_STORAGE_COUNTER = 100;
  private static final int MOCKED_STORAGE_SLEEP = 100;

  private AsyncLimitUsageStorage asyncStorage;
  private LimitUsageStorage mockedStorage;
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
    mockedStorage = mock(LimitUsageStorage.class);
    when(mockedStorage.addAndGet(anyCollectionOf(AddAndGetRequest.class)))
        .then(
            invocation -> {
              Thread.sleep(MOCKED_STORAGE_SLEEP);
              logger.info("Mocked storage returning!");
              return ImmutableMap.of(LimitKey.fromRequest(request), MOCKED_STORAGE_COUNTER);
            });

    asyncStorage = new AsyncLimitUsageStorage(mockedStorage);
  }

  @Test
  public void canAddAndGet() throws InterruptedException {
    int counter = asyncStorage.addAndGet(request).getValue();

    assertThat(counter).isEqualTo(1);
    assertThat(asyncStorage.debugCurrentLimitCounters()).hasSize(0);
    Thread.sleep(MOCKED_STORAGE_SLEEP * 2);
    // After a while, the mockedStorage returns and the counter is overwritten.
    counter = asyncStorage.addAndGet(request).getValue();
    assertThat(counter).isEqualTo(MOCKED_STORAGE_COUNTER + 1);
  }
}
