/**
 * The MIT License
 * Copyright (c) 2016 Coveo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.coveo.spillway;

import com.coveo.spillway.exception.SpillwayLimitExceededException;
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.eq;

public class SpillwayTest {

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
  private Clock clock;

  private SpillwayFactory inMemoryFactory;
  private SpillwayFactory mockedFactory;

  @Before
  public void setup() {
    clock = mock(Clock.class);
    inMemoryFactory = new SpillwayFactory(new InMemoryStorage(), clock);

    mockedStorage = mock(LimitUsageStorage.class);
    mockedFactory = new SpillwayFactory(mockedStorage);

    when(clock.instant()).thenReturn(Instant.now());
  }

  @Test
  public void simpleLimit() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse(); // Third tryCall fails
  }

  @Test
  public void multipleLimitsWithOverlap() {
    Limit<User> limit1 =
        LimitBuilder.of("perUser", User::getName).to(5).per(Duration.ofHours(1)).build();
    Limit<User> limit2 =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofSeconds(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", limit1, limit2);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse(); // 2nd tryCall fails
  }

  @Test
  public void multipleLimitsNoOverlap() {
    Limit<User> ipLimit =
        LimitBuilder.of("perIp", User::getIp).to(1).per(Duration.ofHours(1)).build();
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", ipLimit, userLimit);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(gina)).isFalse(); // Gina is on John's IP.
  }

  @Test
  public void multipleResourcesEachHaveTheirOwnLimit() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
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

    assertThat(spillway.tryCall(john.getName())).isTrue();
    assertThat(spillway.tryCall(john.getName())).isFalse();
  }

  @Test
  public void canBeNotifiedWhenLimitIsExceeded() {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
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
  public void callThrowsAnExceptionWhichContainsAllTheDetails()
      throws SpillwayLimitExceededException {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
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
  public void callThrowsForMultipleBreachedLimits() throws SpillwayLimitExceededException {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofHours(1)).build();
    Limit<User> ipLimit =
        LimitBuilder.of("perIp", User::getIp).to(1).per(Duration.ofHours(1)).build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit, ipLimit);

    spillway.call(john);
    try {
      spillway.call(john);
      fail("Expected an exception!");
    } catch (SpillwayLimitExceededException ex) {
      assertThat(ex.getExceededLimits()).hasSize(2);
      assertThat(ex.getExceededLimits().get(0)).isEqualTo(userLimit.getDefinition());
      assertThat(ex.getExceededLimits().get(1)).isEqualTo(ipLimit.getDefinition());
      assertThat(ex.getContext()).isEqualTo(john);
    }
  }

  @Test
  public void bucketChangesWhenTimePasses() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofSeconds(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse();

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(spillway.tryCall(john)).isTrue();
  }

  @Test
  public void canGetCurrentLimitStatus() throws SpillwayLimitExceededException {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
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
  public void ifCallbackThrowsWeIgnoreThatCallbackAndContinue() {
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

    spillway.tryCall(john);
    spillway.tryCall(john);

    verify(callbackThatThrows).trigger(ipLimit1.getDefinition(), john);
    verify(callbackThatIsOkay).trigger(userLimit.getDefinition(), john);
    verify(callbackThatThrows).trigger(ipLimit2.getDefinition(), john);
  }

  @Test
  public void canExceedLimitByDoingALargeIncrement() {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(2).per(Duration.ofHours(1)).build();
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
        LimitBuilder.of("perUser", User::getName).to(4).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john, 4)).isTrue();
    assertThat(spillway.tryCall(john, 1)).isFalse();
  }

  @Test
  public void canAddLimitTriggers() {
    LimitTriggerCallback callback = mock(LimitTriggerCallback.class);
    ValueThresholdTrigger trigger = new ValueThresholdTrigger(5, callback);

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
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
    when(mockedStorage.addAndGet(anyListOf(AddAndGetRequest.class)))
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
    assertThat(spillway.tryCall(john, 100)).isTrue();

    verify(callback, never()).trigger(any(LimitDefinition.class), any());
  }

  @Test
  public void canAddCapacityLimitOverride() {
    LimitOverride override = LimitOverrideBuilder.of(JOHN).to(A_CAPACITY).per(A_DURATION).build();

    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName)
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
        LimitBuilder.of("perUser", User::getName)
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
        LimitBuilder.of(A_LIMIT_NAME, User::getName)
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
        LimitBuilder.of("perUser", User::getName)
            .to(A_HIGHER_CAPACITY)
            .per(A_DURATION)
            .withLimitOverride(override)
            .withLimitOverride(anotherOverride)
            .build();

    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);
    assertThat(spillway.tryCall(john, A_SMALLER_CAPACITY + 10)).isFalse();
  }
}
