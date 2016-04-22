package com.coveo.spillway;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import static com.google.common.truth.Truth.assertThat;

/**
 * These are slightly functional tests in the sense that they do not mock Redis.
 * The behavior of the RedisStorage class is tightly coupled with the behavior
 * of redis so it makes sense imho.
 */
public class RedisStorageTest {

  private static final String RESOURCE1 = "someResource";
  private static final String LIMIT1 = "someLimit";
  private static final String PROPERTY1 = "someProperty";
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final Instant TIMESTAMP = Instant.now();

  private static final Logger logger = LoggerFactory.getLogger(RedisStorageTest.class);

  private static RedisServer redisServer;
  private static Jedis jedis;
  private static RedisStorage storage;

  @BeforeClass
  public static void startRedis() throws IOException {
    try {
      redisServer = new RedisServer(6389);
    } catch (IOException e) {
      logger.error("Failed to start Redis server. Is port 6389 available?");
      throw e;
    }
    redisServer.start();
    jedis = new Jedis("localhost", 6389);
    storage = new RedisStorage(jedis);
  }

  @AfterClass
  public static void stopRedis() {
    redisServer.stop();
  }

  @Before
  public void flushDataInRedis() {
    jedis.flushDB();
  }

  @Test
  public void canIncrement() {
    int counter = storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    assertThat(counter).isEqualTo(1);
  }

  @Test
  public void canIncrementMultipleTimes() {
    for (int i = 0; i < 10; i++) {
      int result = storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
      assertThat(result).isEqualTo(i + 1);
    }
  }

  @Test
  public void keysCanExpire() throws InterruptedException {
    int result1 =
            storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, Duration.ofSeconds(1), TIMESTAMP);
    assertThat(result1).isEqualTo(1);

    Thread.sleep(2000);

    int result2 =
            storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, Duration.ofSeconds(1), TIMESTAMP);
    assertThat(result2).isEqualTo(1);
  }

  @Test
  public void canDebugLimitCounters() {
    for (int i = 0; i < 10; i++) {
      storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1 + i, EXPIRATION, TIMESTAMP);
    }
    Map<LimitKey, Integer> limitCounters = storage.debugCurrentLimitCounters();
    assertThat(limitCounters).hasSize(10);
    for (Map.Entry<LimitKey, Integer> limitCounter : limitCounters.entrySet()) {
      assertThat(limitCounter.getKey().getResource()).isEqualTo(RESOURCE1);
      assertThat(limitCounter.getKey().getProperty()).startsWith(PROPERTY1);
      assertThat(limitCounter.getKey().getBucket())
              .isGreaterThan(Instant.now().minus(EXPIRATION).minus(EXPIRATION));
      assertThat(limitCounter.getKey().getBucket()).isLessThan(Instant.now());
      assertThat(limitCounter.getValue()).isEqualTo(1);
    }
  }

  @Test
  public void expiredKeysCompletelyDisappear() throws InterruptedException {
    int result1 =
            storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, Duration.ofSeconds(1), TIMESTAMP);

    assertThat(result1).isEqualTo(1);
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);
    Thread.sleep(1000);
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);
    Thread.sleep(2000);
    assertThat(storage.debugCurrentLimitCounters()).hasSize(0);
  }

  @Test
  public void canAddLargeValues() {
    int result = storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5);
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void canAddLargeValuesToExisitingCounters() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    int result = storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5);

    assertThat(result).isEqualTo(6);
  }
}
