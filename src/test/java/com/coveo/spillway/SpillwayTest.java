package com.coveo.spillway;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpillwayTest {

  private static int ONE_MILLION = 1000000;

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

  private User john = new User("john", "127.0.0.1");
  private User gina = new User("gina", "127.0.0.1");

  private LimitUsageStorage mockedStorage;

  private SpillwayFactory inMemoryFactory;
  private SpillwayFactory mockedFactory;

  @Before
  public void setup() {
    inMemoryFactory = new SpillwayFactory(new InMemoryStorage());

    mockedStorage = mock(LimitUsageStorage.class);
    mockedFactory = new SpillwayFactory(mockedStorage);
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
  public void oneMillionConcurrentRequestsWith100Threads() throws InterruptedException {
    Limit<User> ipLimit =
        LimitBuilder.of("perIp", User::getIp).to(ONE_MILLION).per(Duration.ofHours(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", ipLimit);

    ExecutorService threadPool = Executors.newFixedThreadPool(100);

    AtomicInteger counter = new AtomicInteger(0);
    // We do ONE MILLION + 1 iterations and check to make sure that the counter was not incremented more than expected.
    for (int i = 0; i < ONE_MILLION + 1; i++) {
      threadPool.submit(
          () -> {
            boolean canCall = spillway.tryCall(john);
            if (canCall) {
              counter.incrementAndGet();
            }
          });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(1, TimeUnit.MINUTES);

    assertThat(counter.get()).isEqualTo(ONE_MILLION);
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
  public void bucketChangesWhenTimePasses() throws InterruptedException {
    Limit<User> userLimit =
        LimitBuilder.of("perUser", User::getName).to(1).per(Duration.ofSeconds(1)).build();
    Spillway<User> spillway = inMemoryFactory.enforce("testResource", userLimit);

    assertThat(spillway.tryCall(john)).isTrue();
    assertThat(spillway.tryCall(john)).isFalse();

    Thread.sleep(2000); // Sleep two seconds to ensure that we bump to another bucket

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
  public void ifCallbackThrowsWeIgnoreThatCallbackAndContinue()
      throws SpillwayLimitExceededException {
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
  public void canIncrementByALargeNumber() {
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
    when(mockedStorage.addAndGet(anyList())).thenReturn(Arrays.asList(1, 2, 3));

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
}
