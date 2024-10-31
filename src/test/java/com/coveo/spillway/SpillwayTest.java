package com.coveo.spillway;

import com.coveo.spillway.exception.SpillwayLimitExceededException;
import com.coveo.spillway.exception.SpillwayLimitsWithSameNameException;
import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.limit.LimitDefinition;
import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.limit.override.LimitOverride;
import com.coveo.spillway.limit.override.LimitOverrideBuilder;
import com.coveo.spillway.storage.InMemoryStorage;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.trigger.LimitTriggerCallback;
import com.coveo.spillway.trigger.ValueThresholdTrigger;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpillwayTest {

  private static final int A_SMALLER_CAPACITY = 50;
  private static final int A_CAPACITY = 100;
  private static final int A_HIGHER_CAPACITY = 500;
  private static final Duration A_DURATION = Duration.ofHours(1);
  private static final Duration A_SHORT_DURATION = Duration.ofSeconds(2);
  private static final String JOHN = "john";
  private static final String A_LIMIT_NAME = "perUser";

  private record User(String name, String ip) {}

  private final User john = new User(JOHN, "127.0.0.1");
  private final User gina = new User("gina", "127.0.0.1");

  private LimitUsageStorage mockedStorage;
  private LimitUsageStorage inMemoryStorage;
  private Clock clock;

  private SpillwayFactory inMemoryFactory;
  private SpillwayFactory mockedFactory;

  @BeforeEach
  public void setup() {
    clock = mock(Clock.class);
    inMemoryStorage = new InMemoryStorage();
    inMemoryFactory = new SpillwayFactory(inMemoryStorage, clock);

    mockedStorage = mock(LimitUsageStorage.class);
    mockedFactory = new SpillwayFactory(mockedStorage);

    when(clock.instant()).thenReturn(Instant.now());
  }

  @Test
  public void simpleLimit() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse(); // Third tryCall fails
  }

  @Test
  public void multipleLimitsWithOverlap() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(5).per(Duration.ofHours(1)).build();
    Limit<User> limit2 =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofSeconds(1)).build();

    assertThrows(
        SpillwayLimitsWithSameNameException.class,
        () -> inMemoryFactory.enforce("testResource", limit1, limit2));
  }

  @Test
  public void multipleLimitsNoOverlap() {
    Limit<User> ipLimit = LimitBuilder.of("perIp", User::ip).to(1).per(Duration.ofHours(1)).build();
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", ipLimit, userLimit);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(gina)).isFalse(); // Gina is on John's IP.
  }

  @Test
  public void multipleResourcesEachHaveTheirOwnLimit() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway1 = inMemoryFactory.enforce("testResource1", userLimit);
    Spillway<User> spillway2 = inMemoryFactory.enforce("testResource2", userLimit);

    assertThat(spillway1.tryCall(john)).isTrue();
    assertThat(spillway2.tryCall(john)).isTrue();
    assertThat(spillway1.tryCall(john)).isFalse();
    assertThat(spillway2.tryCall(john)).isFalse();
  }

  @Test
  public void canUseDefaultPropertyExtractor() {
    Limit<String> userLimit = LimitBuilder.of("perUser").to(1).per(Duration.ofHours(1)).build();
    Spillway<String> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john.name())).isTrue();
    assertThat(spillway.tryCall(john.name())).isFalse();
  }

  @Test
  public void canBeNotifiedWhenLimitIsExceeded() {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callback)
            .build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.tryCall(john);
    spillway.tryCall(john);

    verify(callback).trigger(userLimit.getDefinition(), john);
  }

  @Test
  public void callThrowsAnExceptionWhichContainsAllTheDetails() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.call(john);
    try {
      spillway.call(john);
      fail("Expected an exception!");
    } catch (SpillwayLimitExceededException ex) {
      assertThat(ex.getExceededLimits()).hasSize(1);
      assertThat(ex.getExceededLimits().get(0)).isEqualTo(userLimit.getDefinition());
      assertThat(ex.getContext()).isEqualTo(john);
    }
  }

  @Test
  public void callThrowsForMultipleBreachedLimits() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofHours(1)).build();
    Limit<User> ipLimit = LimitBuilder.of("perIp", User::ip).to(1).per(Duration.ofHours(1)).build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit, ipLimit);

    spillway.call(john);
    try {
      spillway.call(john);
      fail("Expected an exception!");
    } catch (SpillwayLimitExceededException ex) {
      assertThat(ex.getExceededLimits()).hasSize(2);
      assertThat(ex.getExceededLimits()).contains(userLimit.getDefinition());
      assertThat(ex.getExceededLimits()).contains(ipLimit.getDefinition());
      assertThat(ex.getContext()).isEqualTo(john);
    }
  }

  @Test
  public void bucketChangesWhenTimePasses() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofSeconds(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse();

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(spillway.tryCall(john)).isTrue();
  }

  @Test
  public void capacityNotIncrementedIfLimitTriggered() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(0).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.tryCall(john)).isFalse();

    Map<LimitKey, Integer> counters = inMemoryStorage.getCurrentLimitCounters();

    assertThat(counters.values()).containsExactly(0);
  }

  @Test
  public void canGetCurrentLimitStatus() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.call(john);
    spillway.call(gina);
    spillway.call(john);

    Map<LimitKey, Integer> limitStatuses = spillway.debugCurrentLimitCounters();
    assertThat(limitStatuses).hasSize(2);
    assertThat(limitStatuses.toString()).isNotEmpty();

    Optional<LimitKey> johnKey =
        limitStatuses
            .keySet()
            .stream()
            .filter(key -> key.getProperty().equals(john.name()))
            .findFirst();
    assertThat(johnKey.isPresent()).isTrue();
    assertThat(limitStatuses.get(johnKey.get())).isEqualTo(2);

    Optional<LimitKey> ginaKey =
        limitStatuses
            .keySet()
            .stream()
            .filter(key -> key.getProperty().equals(gina.name()))
            .findFirst();
    assertThat(ginaKey.isPresent()).isTrue();
    assertThat(limitStatuses.get(ginaKey.get())).isEqualTo(1);
  }

  @Test
  public void ifCallbackThrowsWeIgnoreThatCallbackAndContinue() {
    LimitTriggerCallback callbackThatIsOkay = mock(LimitTriggerCallback.class);
    LimitTriggerCallback callbackThatThrows = mock(LimitTriggerCallback.class);
    doThrow(RuntimeException.class)
        .when(callbackThatThrows)
        .trigger(any(LimitDefinition.class), any(Object.class));
    Limit<User> ipLimit1 =
        LimitBuilder.of("perIp1", User::ip)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatThrows)
            .build();
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatIsOkay)
            .build();
    Limit<User> ipLimit2 =
        LimitBuilder.of("perIp2", User::ip)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatThrows)
            .build();
    Spillway<User> spillway =
        inMemoryFactory.enforce("testResource", ipLimit1, userLimit, ipLimit2);

    spillway.tryCall(john);
    spillway.tryCall(john);

    verify(callbackThatThrows).trigger(ipLimit1.getDefinition(), john);
    verify(callbackThatIsOkay).trigger(userLimit.getDefinition(), john);
    verify(callbackThatThrows).trigger(ipLimit2.getDefinition(), john);
  }

  @Test
  public void canExceedLimitByDoingALargeIncrement() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    try {
      spillway.call(john, 3);
      fail("Expected an exception");
    } catch (SpillwayLimitExceededException e) {
      assertThat(e.getExceededLimits()).hasSize(1);
      assertThat(e.getExceededLimits().get(0)).isEqualTo(userLimit.getDefinition());
    }
  }

  @Test
  public void costCanBeALargeNumber() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name).to(4).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john, 4)).isTrue();
    assertThat(spillway.tryCall(john, 1)).isFalse();
  }

  @Test
  public void canAddLimitTriggers() {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    ValueThresholdTrigger trigger = new ValueThresholdTrigger(5, callback);

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(15)
            .per(Duration.ofHours(1))
            .withLimitTrigger(trigger)
            .build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.tryCall(john, 5);
    verify(callback, never()).trigger(userLimit.getDefinition(), john);
    spillway.tryCall(john, 1);
    verify(callback, times(1)).trigger(userLimit.getDefinition(), john);
    // Calling it again does not lead to another alarm.
    spillway.tryCall(john, 1);
    verify(callback, times(1)).trigger(userLimit.getDefinition(), john);
  }

  @Test
  public void triggersAreIgnoreIfTheStorageReturnsAnIncoherentResponse() {
    when(mockedStorage.addAndGet(anyCollection()))
        .thenReturn(
            ImmutableMap.of(
                mock(LimitKey.class), 1, mock(LimitKey.class), 2, mock(LimitKey.class), 3));

    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    ValueThresholdTrigger trigger = new ValueThresholdTrigger(5, callback);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(15)
            .per(Duration.ofHours(1))
            .withLimitTrigger(trigger)
            .build();

    Spillway<User> spillway = mockedFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, 100)).isTrue();

    verify(callback, never()).trigger(any(LimitDefinition.class), any());
  }

  @Test
  public void canAddCapacityLimitOverride() {
    LimitOverride override = LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(A_HIGHER_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, A_CAPACITY + 10)).isFalse();
  }

  @Test
  public void canAddExpirationLimitOverride() {
    LimitOverride override =
        LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_SHORT_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(A_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, A_CAPACITY)).isTrue();
    assertThat(spillway.tryCall(john, 1)).isFalse();

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(spillway.tryCall(john, A_CAPACITY)).isTrue();
  }

  @Test
  public void canAddTriggersLimitOverride() {
    ArgumentCaptor<LimitDefinition> definitionCaptor =
        ArgumentCaptor.forClass(LimitDefinition.class);

    LimitTriggerCallback limitCallback = mock(LimitTriggerCallback.class);
    LimitTriggerCallback overrideCallback = mock(LimitTriggerCallback.class);

    LimitOverride override =
        LimitOverrideBuilder.of(JOHN)
            .to(A_HIGHER_CAPACITY)
            .per(A_SHORT_DURATION)
            .withExceededCallback(overrideCallback)
            .build();

    Limit<User> userLimit =
        LimitBuilder.of(A_LIMIT_NAME, User::name)
            .to(A_CAPACITY)
            .per(A_DURATION)
            .withExceededCallback(limitCallback)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, A_HIGHER_CAPACITY + 10)).isFalse();

    verify(limitCallback, never()).trigger(any(), any());
    verify(overrideCallback).trigger(definitionCaptor.capture(), eq(john));

    assertThat(definitionCaptor.getValue().getCapacity()).isEqualTo(A_HIGHER_CAPACITY);
    assertThat(definitionCaptor.getValue().getExpiration()).isEqualTo(A_SHORT_DURATION);
    assertThat(definitionCaptor.getValue().getName()).isEqualTo(A_LIMIT_NAME);
  }

  @Test
  public void testAddMultipleLimitOverridesForSameProperty() {
    LimitOverride override = LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_DURATION).build();
    LimitOverride anotherOverride =
        LimitOverrideBuilder.of(JOHN).to(A_SMALLER_CAPACITY).per(A_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(A_HIGHER_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .withLimitOverride(anotherOverride)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, A_SMALLER_CAPACITY + 10)).isFalse();
  }

  @Test
  public void positiveCostOnlyCall() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThrows(IllegalArgumentException.class, () -> spillway.call(john, 0));
  }

  @Test
  public void positiveCostOnlyTryCall() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThrows(IllegalArgumentException.class, () -> spillway.tryCall(john, 0));
  }

  @Test
  public void positiveCostOnlyCheckLimit() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThrows(IllegalArgumentException.class, () -> spillway.checkLimit(john, 0));
  }

  @Test
  public void checkLimit() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(3).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.checkLimit(john)).isTrue();
    spillway.call(john, 2);
    assertThat(spillway.checkLimit(john)).isTrue();
    assertThat(spillway.checkLimit(john, 2)).isFalse();
    spillway.call(john);
    assertThat(spillway.checkLimit(john)).isFalse();
  }

  @Test
  public void checkLimitDoesNotAffectStorage() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::name).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    spillway.checkLimit(john);
    Map<LimitKey, Integer> currentCounts = inMemoryStorage.getCurrentLimitCounters("testResource");

    Optional<LimitKey> johnKey =
        currentCounts
            .keySet()
            .stream()
            .filter(key -> key.getProperty().equals(john.name()))
            .findFirst();
    assertThat(johnKey.isPresent()).isTrue();
    assertThat(currentCounts.get(johnKey.get())).isEqualTo(0);
  }

  @Test
  public void checkLimitDoesNotFireTriggers() {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::name)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callback)
            .build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.checkLimit(john);
    spillway.checkLimit(john);

    verify(callback, never()).trigger(userLimit.getDefinition(), john);
  }
}
