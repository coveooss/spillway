package com.coveo.spillway.storage;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static com.google.common.truth.Truth.*;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import static org.mockito.Mockito.any;

public class AsyncBatchLimitUsageStorageTest
{
  private static final Logger logger = LoggerFactory.getLogger(AsyncBatchLimitUsageStorageTest.class);

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
              return ImmutablePair.of(LimitKey.fromRequest(invocation.getArgumentAt(0, AddAndGetRequest.class)), MOCKED_STORAGE_COUNTER);
            });
  }

  //This test is quite tricky, we want to verify that nothing goes wrong if a sync is started and we remove the related key.
  //The storage starting sleep time must then be greater than the first debugCacheLimitCounters snapshot and the end of the
  //sleep must be after the second debugCacheLimitCounters snapshot.
  @Test
  public void asyncBatchStorageTestWithExpiredKeysWorks() throws Exception
  {
    Map<Instant, Map<LimitKey, Integer>> history  = new LinkedHashMap<>();
    
    asyncStorage = new AsyncBatchLimitUsageStorage(mockedStorage, 100, 50);
    
    asyncStorage.incrementAndGet(RESOURCE, LIMITNAME, PROPERTY, EXPIRATION, Instant.now());
    history.put(Instant.now(), asyncStorage.debugCacheLimitCounters());
    
    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.put(Instant.now(), asyncStorage.debugCacheLimitCounters());
    
    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.put(Instant.now(), asyncStorage.debugCacheLimitCounters());
    
    verify(mockedStorage).addAndGet(any(AddAndGetRequest.class));
    
    boolean firstRun = true;
    for(Entry<Instant, Map<LimitKey, Integer>> snapshot : history.entrySet()) {
      System.out.println(snapshot.getKey().toString() + " : ");
      
      if(firstRun) {
        assertThat(snapshot.getValue()).hasSize(1);
        firstRun = false;
      } else {
        assertThat(snapshot.getValue()).hasSize(0);
      }
         
      snapshot.getValue().entrySet().forEach(entry -> {
        System.out.println(entry.getKey().toString() + " : " + entry.getValue());
      });
    }
  }
}
