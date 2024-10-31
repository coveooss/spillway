package com.coveo.spillway.storage.utils;

import static com.google.common.truth.Truth.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.InMemoryStorage;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.google.common.collect.ImmutableMap;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CacheSynchronizationTest {
  private static final String RESOURCE = "TheResource";
  private static final String LIMIT = "TheLimit";
  private static final String PROPERTY = "TheProperty";
  private static final Duration EXPIRATION = Duration.ofDays(1);
  private static final Instant BUCKET = Instant.now();
  private static final Integer COST = 1;

  @Mock private InMemoryStorage inMemoryStorageMock;
  @Mock private LimitUsageStorage limitUsageStorageMock;

  @Captor private ArgumentCaptor<List<OverrideKeyRequest>> listOfOverrideKeyRequestCaptor;
  @Captor private ArgumentCaptor<AddAndGetRequest> addAndGetRequestCaptor;

  private CacheSynchronization cacheSynchronization;

  @BeforeEach
  public void setup() {
    cacheSynchronization = new CacheSynchronization(inMemoryStorageMock, limitUsageStorageMock);
  }

  @Test
  public void testInit() {
    Map<LimitKey, Integer> counters = givenCounters();
    givenLimitUsageStorageHasValues(counters);

    cacheSynchronization.init();

    verify(inMemoryStorageMock).overrideKeys(listOfOverrideKeyRequestCaptor.capture());

    assertThat(listOfOverrideKeyRequestCaptor.getValue()).hasSize(1);
    assertThat(listOfOverrideKeyRequestCaptor.getValue().get(0).getLimitKey())
        .isEqualTo(counters.entrySet().iterator().next().getKey());
    assertThat(listOfOverrideKeyRequestCaptor.getValue().get(0).getNewValue())
        .isEqualTo(counters.entrySet().iterator().next().getValue());
  }

  @Test
  public void testRun() {
    givenInMemoryCacheHasValues(givenCounters());

    cacheSynchronization.run();

    verify(limitUsageStorageMock).addAndGet(addAndGetRequestCaptor.capture());

    AddAndGetRequest addAndGetRequest = addAndGetRequestCaptor.getValue();
    assertThat(addAndGetRequest).isNotNull();
    assertThat(addAndGetRequest.getResource()).isEqualTo(RESOURCE);
    assertThat(addAndGetRequest.getLimitName()).isEqualTo(LIMIT);
    assertThat(addAndGetRequest.getProperty()).isEqualTo(PROPERTY);
    assertThat(addAndGetRequest.getExpiration()).isEqualTo(EXPIRATION);
    assertThat(addAndGetRequest.getEventTimestamp()).isEqualTo(BUCKET);
    assertThat(addAndGetRequest.getCost()).isEqualTo(COST);
  }

  private Map<LimitKey, Integer> givenCounters() {
    return ImmutableMap.of(new LimitKey(RESOURCE, LIMIT, PROPERTY, true, BUCKET, EXPIRATION), COST);
  }

  private void givenLimitUsageStorageHasValues(Map<LimitKey, Integer> counters) {
    when(limitUsageStorageMock.getCurrentLimitCounters()).thenReturn(counters);
  }

  @SuppressWarnings("cast")
  private void givenInMemoryCacheHasValues(Map<LimitKey, Integer> counters) {
    doAnswer(
            invocation -> {
              Consumer<Entry<LimitKey, Capacity>> consumer = invocation.getArgument(0);

              for (Entry<LimitKey, Capacity> entry :
                  counters
                      .entrySet()
                      .stream()
                      .collect(
                          Collectors.toMap(
                              Entry::getKey,
                              entry -> {
                                Capacity capacity = new Capacity();
                                capacity.addAndGet(entry.getValue());

                                return capacity;
                              }))
                      .entrySet()) {
                consumer.accept(entry);
              }

              return null;
            })
        .when(inMemoryStorageMock)
        .applyOnEach(any(Consumer.class));
  }
}
