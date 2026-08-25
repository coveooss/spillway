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
package com.coveo.spillway.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;

/**
 * Implementation of {@link LimitUsageStorage} using a Redis storage.
 * <p>
 * Uses a {@link UnifiedJedis} client to communicate with the database, which supports
 * standalone, sentinel and cluster deployments.
 * It will automatically reconnect to the Redis server in case of connection lost.
 * <p>
 * We suggest to wrap this storage in the {@link AsyncBatchLimitUsageStorage}
 * to avoid slowing down queries if external troubles occurs with the database.
 *
 * @author Guillaume Simard
 * @author Emile Fugulin
 * @author Simon Toussaint
 * @since 1.0.0
 */
public class RedisStorage implements LimitUsageStorage {
  private static final Logger logger = LoggerFactory.getLogger(RedisStorage.class);

  /*package*/ static final String DEFAULT_PREFIX = "spillway";
  /*package*/ static final String KEY_SEPARATOR = "|";

  private static final String KEY_SEPARATOR_SUBSTITUTE = "_";
  private static final String WILD_CARD_OPERATOR = "*";
  private static final String COUNTER_SCRIPT =
      "local counter = redis.call('INCRBY', KEYS[1], ARGV[1]); "
          + "if counter  > tonumber(ARGV[2]) + tonumber(ARGV[1])"
          + "then counter = redis.call('INCRBY', KEYS[1], -ARGV[1]) "
          + "end "
          + "return tostring(counter)";

  private final UnifiedJedis redisClient;
  private final String keyPrefix;

  public RedisStorage(UnifiedJedis redisClient) {
    this(redisClient, DEFAULT_PREFIX);
  }

  public RedisStorage(UnifiedJedis redisClient, String keyPrefix) {
    this.redisClient = Objects.requireNonNull(redisClient);
    if (StringUtils.isBlank(keyPrefix)) {
      throw new IllegalArgumentException("keyPrefix must not be blank.");
    }
    this.keyPrefix = keyPrefix;
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Response<Long>> responses = new LinkedHashMap<>();

    // The bucketing mechanism guarantees that a counter is only ever incremented, so these
    // commands do not need to be wrapped in a transaction and can all be pipelined together.
    try (AbstractPipeline pipeline = redisClient.pipelined()) {
      for (AddAndGetRequest request : requests) {
        LimitKey limitKey = LimitKey.fromRequest(request);
        String redisKey = buildKey(limitKey);

        responses.put(limitKey, pipeline.incrBy(redisKey, request.getCost()));
        // We set the expire to twice the expiration period. The expiration is there to ensure that we don't fill the Redis cluster with
        // useless keys. The actual expiration mechanism is handled by the bucketing mechanism.
        pipeline.expire(redisKey, request.getExpiration().getSeconds() * 2);
      }

      pipeline.sync();
    } catch (Throwable e) {
      logger.error("An exception occurred while publishing limits to Redis.", e);
    }

    return responses
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kvp -> kvp.getValue().get().intValue()));
  }

  @Override
  public Map<LimitKey, Integer> addAndGetWithLimit(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Response<Object>> responses = new LinkedHashMap<>();

    // The counter script is already atomic, so no transaction is required and all the
    // requests can be pipelined together.
    try (AbstractPipeline pipeline = redisClient.pipelined()) {
      for (AddAndGetRequest request : requests) {
        LimitKey limitKey = LimitKey.fromRequest(request);
        String redisKey = buildKey(limitKey);

        responses.put(
            limitKey,
            pipeline.eval(
                COUNTER_SCRIPT,
                Collections.singletonList(redisKey),
                Arrays.asList(
                    String.valueOf(request.getCost()), String.valueOf(request.getLimit()))));
        pipeline.expire(redisKey, request.getExpiration().getSeconds() * 2);
      }

      pipeline.sync();
    } catch (Throwable e) {
      logger.error("An exception occurred while publishing limits to Redis.", e);
    }

    return responses
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, kvp -> Integer.parseInt(kvp.getValue().get().toString())));
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters() {
    return getLimits(buildKeyPattern(keyPrefix, WILD_CARD_OPERATOR));
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(String resource) {
    return getLimits(buildKeyPattern(keyPrefix, resource, WILD_CARD_OPERATOR));
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(String resource, String limitName) {
    return getLimits(buildKeyPattern(keyPrefix, resource, limitName, WILD_CARD_OPERATOR));
  }

  @Override
  public Map<LimitKey, Integer> getCurrentLimitCounters(
      String resource, String limitName, String property) {
    return getLimits(buildKeyPattern(keyPrefix, resource, limitName, property, WILD_CARD_OPERATOR));
  }

  private Map<LimitKey, Integer> getLimits(String keyPattern) {
    Map<LimitKey, Integer> counters = new HashMap<>();

    Set<String> keys = redisClient.keys(keyPattern);
    for (String key : keys) {
      String valueAsString = redisClient.get(key);
      if (StringUtils.isNotEmpty(valueAsString)) {
        int value = Integer.parseInt(valueAsString);

        String[] keyComponents = StringUtils.split(key, KEY_SEPARATOR);

        counters.put(
            new LimitKey(
                keyComponents[1],
                keyComponents[2],
                keyComponents[3],
                true,
                Instant.parse(keyComponents[4]),
                keyComponents.length == 6
                    ? Duration.parse(keyComponents[5])
                    : Duration
                        .ZERO), // Version pre alpha.3 are not storing the expiration within the key so we fallback to 0
            value);
      } else {
        logger.info("Key '{}' has no value and will not be included in counters", key);
      }
    }
    return Collections.unmodifiableMap(counters);
  }

  /**
   * Closes the underlying {@link UnifiedJedis} client, which is therefore not usable anymore.
   */
  @Override
  public void close() {
    redisClient.close();
  }

  private String buildKey(LimitKey limitKey) {
    return buildKeyPattern(
        keyPrefix,
        limitKey.getResource(),
        limitKey.getLimitName(),
        limitKey.getProperty(),
        limitKey.getBucket().toString(),
        limitKey.getExpiration().toString());
  }

  private String buildKeyPattern(String... keyComponents) {
    return Arrays.asList(keyComponents)
        .stream()
        .map(RedisStorage::clean)
        .collect(Collectors.joining(KEY_SEPARATOR));
  }

  private static String clean(String keyComponent) {
    return keyComponent.replace(KEY_SEPARATOR, KEY_SEPARATOR_SUBSTITUTE);
  }
}
