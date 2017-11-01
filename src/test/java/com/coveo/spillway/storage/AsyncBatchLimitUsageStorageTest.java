package com.coveo.spillway.storage;

import static com.google.common.truth.Truth.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.CacheSynchronization;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class AsyncBatchLimitUsageStorageTest {
  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Duration EXPIRATION = Duration.ofSeconds(5);
  private static final Duration TIME_BETWEEN_SYNCHRONIZATIONS = Duration.ofSeconds(1);

  @Mock private LimitUsageStorage storageMock;
  @Spy private InMemoryStorage cacheSpy = new InMemoryStorage();

  private CacheSynchronization cacheSynchronizationSpy;

  @Captor private ArgumentCaptor<Collection<AddAndGetRequest>> addAndGetRequestsCaptor;

  @Before
  public void setup() {
    cacheSpy.map.clear();

    cacheSynchronizationSpy = Mockito.spy(new CacheSynchronization(cacheSpy, storageMock));
  }

  @Test
  public void testCacheIsInitializedWhenEntryIsNotInCache() throws Exception {
    // Given entry is not in cache

    AsyncBatchLimitUsageStorage asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock,
            cacheSpy,
            cacheSynchronizationSpy,
            TIME_BETWEEN_SYNCHRONIZATIONS,
            Duration.ZERO);

    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(1));
    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(2));

    verify(cacheSpy, times(3))
        .addAndGet(
            addAndGetRequestsCaptor
                .capture()); // Two request plus the zero cost request on cache miss

    List<Collection<AddAndGetRequest>> allAddAndGetRequests =
        addAndGetRequestsCaptor.getAllValues();
    Collection<AddAndGetRequest> zeroCostRequests = allAddAndGetRequests.get(0);
    assertThat(zeroCostRequests).hasSize(1);
    assertThat(zeroCostRequests.iterator().next().getCost()).isEqualTo(0);

    Collection<AddAndGetRequest> realRequests1 = allAddAndGetRequests.get(1);
    assertThat(realRequests1).hasSize(1);
    assertThat(realRequests1.iterator().next().getCost()).isEqualTo(1);

    Collection<AddAndGetRequest> realRequests2 = allAddAndGetRequests.get(2);
    assertThat(realRequests2).hasSize(1);
    assertThat(realRequests2.iterator().next().getCost()).isEqualTo(2);

    verify(cacheSynchronizationSpy, times(2))
        .run(); // Should have been called at storage creation and at cache miss
  }

  @Test
  public void testCacheIsNotInitializedWhenEntryIsInCache() throws Exception {
    givenCacheHasLimit(givenDefaultAddAndGetRequest(1));

    AsyncBatchLimitUsageStorage asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock,
            cacheSpy,
            cacheSynchronizationSpy,
            TIME_BETWEEN_SYNCHRONIZATIONS,
            Duration.ZERO);

    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(1));
    asyncBatchLimitUsageStorage.addAndGet(givenDefaultAddAndGetRequest(2));

    verify(cacheSpy, times(2))
        .addAndGet(
            addAndGetRequestsCaptor
                .capture()); // Two request plus the zero cost request on cache miss

    List<Collection<AddAndGetRequest>> allAddAndGetRequests =
        addAndGetRequestsCaptor.getAllValues();

    Collection<AddAndGetRequest> realRequests1 = allAddAndGetRequests.get(0);
    assertThat(realRequests1).hasSize(1);
    assertThat(realRequests1.iterator().next().getCost()).isEqualTo(1);

    Collection<AddAndGetRequest> realRequests2 = allAddAndGetRequests.get(1);
    assertThat(realRequests2).hasSize(1);
    assertThat(realRequests2.iterator().next().getCost()).isEqualTo(2);

    verify(cacheSynchronizationSpy, times(1))
        .run(); // Should have been called at storage creation and at cache miss
  }

  @Test
  public void testSynchronizeIsNotAffectingProcess() throws Exception {
    AsyncBatchLimitUsageStorage asyncBatchLimitUsageStorage =
        new AsyncBatchLimitUsageStorage(
            storageMock, cacheSpy, cacheSynchronizationSpy, Duration.ofMillis(100), Duration.ZERO);

    IntStream.range(0, 1000)
        .forEach(
            i
                -> asyncBatchLimitUsageStorage.addAndGet(
                    givenDefaultAddAndGetRequest(Duration.ofMillis(100))));

    verify(cacheSynchronizationSpy, atLeastOnce()).run();
    verify(storageMock, atLeastOnce()).addAndGet(any(AddAndGetRequest.class));
  }

  private AddAndGetRequest givenDefaultAddAndGetRequest(int cost) {
    return givenAddAndGetRequest(
        RESOURCE, LIMITNAME, PROPERTY, true, EXPIRATION, Instant.now(), cost);
  }

  private AddAndGetRequest givenDefaultAddAndGetRequest(Duration expiration) {
    return givenAddAndGetRequest(RESOURCE, LIMITNAME, PROPERTY, true, expiration, Instant.now(), 1);
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

  private void givenCacheHasLimit(AddAndGetRequest request) {
    when(cacheSpy.getCurrentLimitCounters())
        .thenReturn(ImmutableMap.of(LimitKey.fromRequest(request), 0));
  }
}
