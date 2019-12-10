package com.coveo.spillway;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.trigger.LimitTriggerCallback;
import com.coveo.spillway.trigger.ValueThresholdTrigger;
import com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SpillwayTryUpdateAndVerifyLimitTest {

  private static final int A_SMALLER_CAPACITY = 50;
  private static final int A_CAPACITY = 100;
  private static final int A_HIGHER_CAPACITY = 500;
  private static final Duration A_DURATION = Duration.ofHours(1);
  private static final Duration A_SHORT_DURATION = Duration.ofSeconds(2);
  private static final String JOHN = "john";
  private static final String A_LIMIT_NAME = "perUser";

  private class User {

    private String name;
    private String ip;

    User(String name, String ip) {
      this.name = name;
      this.ip = ip;
    }

    String getName() {
      return name;
    }

    String getIp() {
      return ip;
    }
  }

  private User john = new User(JOHN, "127.0.0.1");
  private User gina = new User("gina", "127.0.0.1");

  private LimitUsageStorage mockedStorage;
  private LimitUsageStorage inMemoryStorage;
  private Clock clock;

  private SpillwayFactory inMemoryFactory;
  private SpillwayFactory mockedFactory;

  @Before
  public void setup() {
    clock = mock(Clock.class);
    inMemoryStorage = new InMemoryStorage();
    inMemoryFactory = new SpillwayFactory(inMemoryStorage, clock);

    mockedStorage = mock(LimitUsageStorage.class);
    mockedFactory = new SpillwayFactory(mockedStorage);

    when(clock.instant()).thenReturn(Instant.now());
  }

  @Test
  public void simpleLimit() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john))
        .isFalse(); // Third tryUpdateAndVerifyLimit fails
  }

  @Test(expected = SpillwayLimitsWithSameNameException.class)
  public void multipleLimitsWithOverlap() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(5).per(Duration.ofHours(1)).build();
    Limit<User> limit2 =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofSeconds(1)).build();
    inMemoryFactory.enforce("testResource", limit1, limit2);
  }

  @Test
  public void multipleLimitsNoOverlap() throws Exception {
    Limit<User> ipLimit =
        LimitBuilder.of("perIp", User::getIp).to(1).per(Duration.ofHours(1)).build();
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", ipLimit, userLimit);

    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(gina)).isFalse(); // Gina is on John's IP.
  }

  @Test
  public void multipleResourcesEachHaveTheirOwnLimit() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway1 = inMemoryFactory.enforce("testResource1", userLimit);
    Spillway<User> spillway2 = inMemoryFactory.enforce("testResource2", userLimit);

    assertThat(spillway1.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway2.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway1.tryUpdateAndVerifyLimit(john)).isFalse();
    assertThat(spillway2.tryUpdateAndVerifyLimit(john)).isFalse();
  }

  @Test
  public void canUseDefaultPropertyExtractor() throws Exception {
    Limit<String> userLimit = LimitBuilder.of("perUser").to(1).per(Duration.ofHours(1)).build();
    Spillway<String> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryUpdateAndVerifyLimit(john.getName())).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john.getName())).isFalse();
  }

  @Test
  public void canBeNotifiedWhenLimitIsExceeded() throws Exception {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callback)
            .build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.tryUpdateAndVerifyLimit(john);
    spillway.tryUpdateAndVerifyLimit(john);

    verify(callback).trigger(userLimit.getDefinition(), john);
  }

  @Test
  public void callThrowsAnExceptionWhichContainsAllTheDetails() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.updateAndVerifyLimit(john);
    try {
      spillway.updateAndVerifyLimit(john);
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
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Limit<User> ipLimit =
        LimitBuilder.of("perIp", User::getIp).to(1).per(Duration.ofHours(1)).build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit, ipLimit);

    spillway.updateAndVerifyLimit(john);
    try {
      spillway.updateAndVerifyLimit(john);
      fail("Expected an exception!");
    } catch (SpillwayLimitExceededException ex) {
      assertThat(ex.getExceededLimits()).hasSize(2);
      assertThat(ex.getExceededLimits()).contains(userLimit.getDefinition());
      assertThat(ex.getExceededLimits()).contains(ipLimit.getDefinition());
      assertThat(ex.getContext()).isEqualTo(john);
    }
  }

  @Test
  public void bucketChangesWhenTimePasses() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofSeconds(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isFalse();

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isTrue();
  }

  @Test
  public void capacityNotIncrementedIfLimitTriggered() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(0).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.tryUpdateAndVerifyLimit(john)).isFalse();

    Map<LimitKey, Integer> counters = inMemoryStorage.getCurrentLimitCounters();

    assertThat(counters.values()).containsExactly(1);
  }

  @Test
  public void canGetCurrentLimitStatus() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.updateAndVerifyLimit(john);
    spillway.updateAndVerifyLimit(gina);
    spillway.updateAndVerifyLimit(john);

    Map<LimitKey, Integer> limitStatuses = spillway.debugCurrentLimitCounters();
    assertThat(limitStatuses).hasSize(2);
    assertThat(limitStatuses.toString()).isNotEmpty();

    Optional<LimitKey> johnKey =
        limitStatuses
            .keySet()
            .stream()
            .filter(key -> key.getProperty().equals(john.getName()))
            .findFirst();
    assertThat(johnKey.isPresent()).isTrue();
    assertThat(limitStatuses.get(johnKey.get())).isEqualTo(2);

    Optional<LimitKey> ginaKey =
        limitStatuses
            .keySet()
            .stream()
            .filter(key -> key.getProperty().equals(gina.getName()))
            .findFirst();
    assertThat(ginaKey.isPresent()).isTrue();
    assertThat(limitStatuses.get(ginaKey.get())).isEqualTo(1);
  }

  @Test
  public void ifCallbackThrowsWeIgnoreThatCallbackAndContinue() throws Exception {
    LimitTriggerCallback callbackThatIsOkay = mock(LimitTriggerCallback.class);
    LimitTriggerCallback callbackThatThrows = mock(LimitTriggerCallback.class);
    doThrow(RuntimeException.class)
        .when(callbackThatThrows)
        .trigger(any(LimitDefinition.class), any(Object.class));
    Limit<User> ipLimit1 =
        LimitBuilder.of("perIp1", User::getIp)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatThrows)
            .build();
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatIsOkay)
            .build();
    Limit<User> ipLimit2 =
        LimitBuilder.of("perIp2", User::getIp)
            .to(1)
            .per(Duration.ofHours(1))
            .withExceededCallback(callbackThatThrows)
            .build();
    Spillway<User> spillway =
        inMemoryFactory.enforce("testResource", ipLimit1, userLimit, ipLimit2);

    spillway.tryUpdateAndVerifyLimit(john);
    spillway.tryUpdateAndVerifyLimit(john);

    verify(callbackThatThrows).trigger(ipLimit1.getDefinition(), john);
    verify(callbackThatIsOkay).trigger(userLimit.getDefinition(), john);
    verify(callbackThatThrows).trigger(ipLimit2.getDefinition(), john);
  }

  @Test
  public void canExceedLimitByDoingALargeIncrement() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    try {
      spillway.updateAndVerifyLimit(john, 3);
      fail("Expected an exception");
    } catch (SpillwayLimitExceededException e) {
      assertThat(e.getExceededLimits()).hasSize(1);
      assertThat(e.getExceededLimits().get(0)).isEqualTo(userLimit.getDefinition());
    }
  }

  @Test
  public void costCanBeALargeNumber() throws Exception {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(4).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryUpdateAndVerifyLimit(john, 4)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john, 1)).isFalse();
  }

  @Test
  public void canAddLimitTriggers() throws Exception {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    ValueThresholdTrigger trigger = new ValueThresholdTrigger(5, callback);

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(15)
            .per(Duration.ofHours(1))
            .withLimitTrigger(trigger)
            .build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    spillway.tryUpdateAndVerifyLimit(john, 5);
    verify(callback, never()).trigger(userLimit.getDefinition(), john);
    spillway.tryUpdateAndVerifyLimit(john, 1);
    verify(callback, times(1)).trigger(userLimit.getDefinition(), john);
    // Calling it again does not lead to another alarm.
    spillway.tryUpdateAndVerifyLimit(john, 1);
    verify(callback, times(1)).trigger(userLimit.getDefinition(), john);
  }

  @Test
  public void triggersAreIgnoreIfTheStorageReturnsAnIncoherentResponse() throws Exception {
    when(mockedStorage.addAndGetWithLimit(anyListOf(AddAndGetRequest.class)))
        .thenReturn(
            ImmutableMap.of(
                mock(LimitKey.class), 1, mock(LimitKey.class), 2, mock(LimitKey.class), 3));

    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    ValueThresholdTrigger trigger = new ValueThresholdTrigger(5, callback);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(15)
            .per(Duration.ofHours(1))
            .withLimitTrigger(trigger)
            .build();

    Spillway<User> spillway = mockedFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryUpdateAndVerifyLimit(john, 100)).isTrue();

    verify(callback, never()).trigger(any(LimitDefinition.class), any());
  }

  @Test
  public void canAddCapacityLimitOverride() throws Exception {
    LimitOverride override = LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(A_HIGHER_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryUpdateAndVerifyLimit(john, A_CAPACITY + 10)).isFalse();
  }

  @Test
  public void canAddExpirationLimitOverride() throws Exception {
    LimitOverride override =
        LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_SHORT_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(A_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryUpdateAndVerifyLimit(john, A_CAPACITY)).isTrue();
    assertThat(spillway.tryUpdateAndVerifyLimit(john, 1)).isFalse();

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(spillway.tryUpdateAndVerifyLimit(john, A_CAPACITY)).isTrue();
  }

  @Test
  public void canAddTriggersLimitOverride() throws Exception {
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
        LimitBuilder.of(A_LIMIT_NAME, User::getName)
            .to(A_CAPACITY)
            .per(A_DURATION)
            .withExceededCallback(limitCallback)
            .withLimitOverride(override)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryUpdateAndVerifyLimit(john, A_HIGHER_CAPACITY + 10)).isFalse();

    verify(limitCallback, never()).trigger(any(), any());
    verify(overrideCallback).trigger(definitionCaptor.capture(), eq(john));

    assertThat(definitionCaptor.getValue().getCapacity()).isEqualTo(A_HIGHER_CAPACITY);
    assertThat(definitionCaptor.getValue().getExpiration()).isEqualTo(A_SHORT_DURATION);
    assertThat(definitionCaptor.getValue().getName()).isEqualTo(A_LIMIT_NAME);
  }

  @Test
  public void testAddMultipleLimitOverridesForSameProperty() throws Exception {
    LimitOverride override = LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_DURATION).build();
    LimitOverride anotherOverride =
        LimitOverrideBuilder.of(JOHN).to(A_SMALLER_CAPACITY).per(A_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
            .to(A_HIGHER_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .withLimitOverride(anotherOverride)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryUpdateAndVerifyLimit(john, A_SMALLER_CAPACITY + 10)).isFalse();
  }

  @Test(expected = IllegalArgumentException.class)
  public void positiveCostOnlyCall() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    spillway.call(john, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void positiveCostOnlyTryCall() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    spillway.tryUpdateAndVerifyLimit(john, 0);
  }

  @Test
  public void checkLimit() throws Exception {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(3).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.checkLimit(john)).isTrue();
    spillway.updateAndVerifyLimit(john, 2);
    assertThat(spillway.checkLimit(john)).isTrue();
    assertThat(spillway.checkLimit(john, 2)).isFalse();
    spillway.updateAndVerifyLimit(john);
    assertThat(spillway.checkLimit(john)).isFalse();
  }

  @Test
  public void tryUpdateAndVerifyLimitMultiThread() throws Exception {
    IntStream.range(0, 10)
        .forEach(
            i -> {
              InMemoryStorage inMemoryStorage = new InMemoryStorage();
              SpillwayFactory inMemoryFactory = new SpillwayFactory(inMemoryStorage);
              Limit<User> userLimit =
                  LimitBuilder.of("perUser", User::getName).to(100).per(A_DURATION).build();
              Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
              Optional<Boolean> limitReached =
                  IntStream.range(0, 101)
                      .parallel()
                      .mapToObj(j -> spillway.tryUpdateAndVerifyLimit(john))
                      .reduce(Boolean::logicalAnd);
              Assert.assertFalse(limitReached.orElse(true));
            });
  }
}
