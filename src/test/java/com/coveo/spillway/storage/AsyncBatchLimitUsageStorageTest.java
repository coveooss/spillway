package com.coveo.spillway.storage;

import static com.google.common.truth.Truth.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.CacheSynchronization;

@ExtendWith(MockitoExtension.class)
public class AsyncBatchLimitUsageStorageTest {
  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Duration EXPIRATION = Duration.ofSeconds(5);
  private static final Duration TIME_BETWEEN_SYNCHRONIZATIONS = Duration.ofSeconds(1);

  private static final int MOCKED_STORAGE_SLEEP = 300;

  @Mock private LimitUsageStorage storageMock;

  private InMemoryStorage cacheSpy;
  private CacheSynchronization cacheSynchronizationSpy;

  @Captor private ArgumentCaptor<Collection<AddAndGetRequest>> addAndGetRequestsCaptor;

  private AsyncBatchLimitUsageStorage asyncBatchLimitUsageStorage;

  @BeforeEach
  public void setup() {
    cacheSpy = Mockito.spy(new InMemoryStorage());
    cacheSynchronizationSpy = Mockito.spy(new CacheSynchronization(cacheSpy, storageMock));
  }

  @AfterEach
  public void tearDown() throws Exception {
    asyncBatchLimitUsageStorage.close();
  }

  @Test
  public void testAddWithCacheForcefullyInitialized() {
    asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock,
            cacheSpy,
            cacheSynchronizationSpy,
            TIME_BETWEEN_SYNCHRONIZATIONS,
            Duration.ZERO,
            true);

    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(1));
    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(2));

    verify(cacheSpy, times(2)).addAndGet(addAndGetRequestsCaptor.capture());

    List<Collection<AddAndGetRequest>> allAddAndGetRequests =
        addAndGetRequestsCaptor.getAllValues();
    Collection<AddAndGetRequest> realRequests1 = allAddAndGetRequests.get(0);
    assertThat(realRequests1).hasSize(1);
    assertThat(realRequests1.iterator().next().getCost()).isEqualTo(1);

    Collection<AddAndGetRequest> realRequests2 = allAddAndGetRequests.get(1);
    assertThat(realRequests2).hasSize(1);
    assertThat(realRequests2.iterator().next().getCost()).isEqualTo(2);

    verify(cacheSynchronizationSpy).init(); // Cache is initialized
  }

  @Test
  public void testAddWithCacheNotForcefullyInitialized() {
    asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock,
            cacheSpy,
            cacheSynchronizationSpy,
            TIME_BETWEEN_SYNCHRONIZATIONS,
            Duration.ZERO,
            false);

    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(1));
    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(2));

    verify(cacheSpy, times(2)).addAndGet(addAndGetRequestsCaptor.capture());

    List<Collection<AddAndGetRequest>> allAddAndGetRequests =
        addAndGetRequestsCaptor.getAllValues();
    Collection<AddAndGetRequest> realRequests1 = allAddAndGetRequests.get(0);
    assertThat(realRequests1).hasSize(1);
    assertThat(realRequests1.iterator().next().getCost()).isEqualTo(1);

    Collection<AddAndGetRequest> realRequests2 = allAddAndGetRequests.get(1);
    assertThat(realRequests2).hasSize(1);
    assertThat(realRequests2.iterator().next().getCost()).isEqualTo(2);

    verify(cacheSynchronizationSpy, never()).init(); // Cache is not initialized
  }

  //This test is quite tricky, we want to verify that nothing goes wrong if a sync is started and we remove the related key.
  //The storage starting sleep time must then be greater than the first debugCacheLimitCounters snapshot and the end of the
  //sleep must be after the second debugCacheLimitCounters snapshot.
  @Test
  public void testSynchronizeIsNotAffectingProcess() throws Exception {
    when(storageMock.addAndGet(any(AddAndGetRequest.class)))
        .then(
            invocation -> {
              Thread.sleep(MOCKED_STORAGE_SLEEP);
              return ImmutablePair.of(LimitKey.fromRequest(invocation.getArgument(0)), 100);
            });

    asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock,
            cacheSpy,
            cacheSynchronizationSpy,
            Duration.ofMillis(MOCKED_STORAGE_SLEEP),
            Duration.ofMillis(MOCKED_STORAGE_SLEEP / 2),
            false);

    asyncBatchLimitUsageStorage.incrementAndGet(
        RESOURCE,
        LIMITNAME,
        PROPERTY,
        true,
        Duration.ofMillis(MOCKED_STORAGE_SLEEP),
        Instant.now());

    List<Entry<Instant, Map<LimitKey, Integer>>> history = new LinkedList<>();
    history.add(
        new SimpleImmutableEntry<>(
            Instant.now(), asyncBatchLimitUsageStorage.debugCacheLimitCounters()));

    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.add(
        new SimpleImmutableEntry<>(
            Instant.now(), asyncBatchLimitUsageStorage.debugCacheLimitCounters()));

    Thread.sleep(MOCKED_STORAGE_SLEEP);
    history.add(
        new SimpleImmutableEntry<>(
            Instant.now(), asyncBatchLimitUsageStorage.debugCacheLimitCounters()));

    verify(storageMock).addAndGet(any(AddAndGetRequest.class));

    assertThat(history.get(0).getValue()).hasSize(1);
    assertThat(history.get(1).getValue()).isEmpty();
    assertThat(history.get(2).getValue()).isEmpty();
  }

  private AddAndGetRequest givenDefaultAddAndGetRequest(int cost) {
    return givenAddAndGetRequest(
        RESOURCE, LIMITNAME, PROPERTY, true, EXPIRATION, Instant.now(), cost);
  }

  private AddAndGetRequest givenAddAndGetRequest(
      String resource,
      String limitName,
      String property,
      boolean distributed,
      Duration expiration,
      Instant eventTimestamp,
      int cost) {
    return new AddAndGetRequest.Builder()
        .withResource(resource)
        .withLimitName(limitName)
        .withProperty(property)
        .withDistributed(distributed)
        .withExpiration(expiration)
        .withEventTimestamp(eventTimestamp)
        .withCost(cost)
        .build();
  }
}
