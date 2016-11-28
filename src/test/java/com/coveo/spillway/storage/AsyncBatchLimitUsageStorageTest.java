package com.coveo.spillway.storage;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static com.google.common.truth.Truth.*;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleImmutableEntry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import static org.mockito.Mockito.any;

public class AsyncBatchLimitUsageStorageTest {
  private static final Logger logger =
      LoggerFactory.getLogger(AsyncBatchLimitUsageStorageTest.class);

  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Duration EXPIRATION = Duration.ofMillis(100);
  private static final int MOCKED_STORAGE_COUNTER = 100;
  private static final int MOCKED_STORAGE_SLEEP = 100;

  private AsyncBatchLimitUsageStorage asyncStorage;
  private LimitUsageStorage mockedStorage;

  @Before
  public void setup() {
    mockedStorage = mock(LimitUsageStorage.class);
    when(mockedStorage.addAndGet(any(AddAndGetRequest.class)))
        .then(
            invocation -> {
              logger.info("Mocked storage sleeping!");
              Thread.sleep(MOCKED_STORAGE_SLEEP);
              logger.info("Mocked storage returning!");
              return ImmutablePair.of(
                  LimitKey.fromRequest(invocation.getArgumentAt(0, AddAndGetRequest.class)),
                  MOCKED_STORAGE_COUNTER);
            });
  }

  //This test is quite tricky, we want to verify that nothing goes wrong if a sync is started and we remove the related key.
  //The storage starting sleep time must then be greater than the first debugCacheLimitCounters snapshot and the end of the
  //sleep must be after the second debugCacheLimitCounters snapshot.
  @Test
  public void asyncBatchStorageTestWithExpiredKeysWorks() throws Exception {

    List<Entry<Instant, Map<LimitKey, Integer>>> history = new LinkedList<>();

    asyncStorage =
        new AsyncBatchLimitUsageStorage(
            mockedStorage, Duration.ofMillis(100), Duration.ofMillis(50));

    asyncStorage.incrementAndGet(RESOURCE, LIMITNAME, PROPERTY, true, EXPIRATION, Instant.now());
    history.add(new SimpleImmutableEntry<>(Instant.now(), asyncStorage.debugCacheLimitCounters()));

    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.add(new SimpleImmutableEntry<>(Instant.now(), asyncStorage.debugCacheLimitCounters()));

    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.add(new SimpleImmutableEntry<>(Instant.now(), asyncStorage.debugCacheLimitCounters()));

    verify(mockedStorage).addAndGet(any(AddAndGetRequest.class));

    assertThat(history.get(0).getValue()).hasSize(1);
    assertThat(history.get(1).getValue()).isEmpty();
    assertThat(history.get(2).getValue()).isEmpty();

    history.forEach(
        snapshot -> {
          logger.debug(snapshot.getKey().toString() + " :");
          snapshot
              .getValue()
              .entrySet()
              .forEach(
                  entry -> {
                    logger.debug(entry.getKey().toString() + " : " + entry.getValue());
                  });
        });
  }
}
