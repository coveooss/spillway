package com.coveo.spillway.storage.sliding;

import static com.google.common.truth.Truth.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.coveo.spillway.storage.sliding.InMemorySlidingStorage;

@RunWith(MockitoJUnitRunner.class)
public class InMemorySlidingStorageTest {
  private static final String RESOURCE1 = "someResource";
  private static final String LIMIT1 = "someLimit";
  private static final String PROPERTY1 = "someProperty";
  private static final Duration EXPIRATION = Duration.ofMinutes(1);
  private static final Duration SLIDE_SIZE = Duration.ofSeconds(1);
  private static final Instant TIMESTAMP = Instant.now();

  @Mock private Clock clock;

  @InjectMocks
  private InMemorySlidingStorage storage = new InMemorySlidingStorage(EXPIRATION, SLIDE_SIZE);

  @Before
  public void setup() {
    when(clock.instant()).thenReturn(Instant.now());
  }

  @Test
  public void canAddValues() {
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5).getValue();
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void canAddValuesToExisitingCounters() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5).getValue();

    assertThat(result).isEqualTo(6);
  }

  @Test
  public void canAddValuesInAnotherSlice() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    int result =
        storage
            .addAndGet(
                RESOURCE1,
                LIMIT1,
                PROPERTY1,
                EXPIRATION,
                TIMESTAMP.plus(SLIDE_SIZE.multipliedBy(2)),
                5)
            .getValue();

    assertThat(result).isEqualTo(6);
  }

  @Test
  public void oldValuesAreRemoved() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP).getValue();
    storage
        .incrementAndGet(
            RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP.plus(EXPIRATION.dividedBy(2)))
        .getValue();

    when(clock.instant()).thenReturn(TIMESTAMP.plus(EXPIRATION));

    int result =
        storage
            .incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP.plus(EXPIRATION))
            .getValue();
    assertThat(result).isEqualTo(2);
  }
}
