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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;

/**
 * Implementation of {@link LimitUsageStorage} using a Redis storage.
 * <p>
 * Uses a {@link Jedis} client to communicate with the database.
 * It will automatically reconnect to the Redis server in case of connection lost.
 * <p>
 * We suggest to wrap this storage in the {@link AsyncBatchLimitUsageStorage}
 * to avoid slowing down queries if external troubles occurs with the database.
 *
 * @author Guillaume Simard
 * @author Emile Fugulin
 * @since 1.0.0
 */
public class RedisStorage implements LimitUsageStorage {

  private static final String KEY_SEPARATOR = "|";
  private static final String DEFAULT_PREFIX = "spillway";

  private final JedisPool jedisPool;
  private final String keyPrefix;
  private final Duration keyDuration;

  public RedisStorage(URI uri) {
    this(uri, DEFAULT_PREFIX);
  }

  public RedisStorage(URI uri, String keyPrefix) {
    this(uri, keyPrefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisStorage(URI uri, String keyPrefix, int timeout) {
    this(new GenericObjectPoolConfig(), uri, keyPrefix, timeout);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, URI uri) {
    this(poolConfig, uri, DEFAULT_PREFIX);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, URI uri, String keyPrefix) {
    this(poolConfig, uri, keyPrefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, URI uri, String keyPrefix, int timeout) {
    this(poolConfig, uri, keyPrefix, timeout, Duration.ZERO);
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig,
      URI uri,
      String keyPrefix,
      int timeout,
      Duration keyDuration) {
    this.jedisPool = new JedisPool(poolConfig, uri, timeout);
    this.keyPrefix = keyPrefix;
    this.keyDuration = keyDuration;
  }

  public RedisStorage(String host) {
    this(host, Protocol.DEFAULT_PORT);
  }

  public RedisStorage(String host, int port) {
    this(host, port, DEFAULT_PREFIX);
  }

  public RedisStorage(String host, int port, String keyPrefix) {
    this(new GenericObjectPoolConfig(), host, port, keyPrefix);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, String host) {
    this(poolConfig, host, Protocol.DEFAULT_PORT);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, String host, int port) {
    this(poolConfig, host, port, DEFAULT_PREFIX);
  }

  public RedisStorage(GenericObjectPoolConfig poolConfig, String host, int port, String keyPrefix) {
    this(poolConfig, host, port, keyPrefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig, String host, int port, String keyPrefix, int timeout) {
    this(poolConfig, host, port, keyPrefix, timeout, Duration.ZERO);
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      String keyPrefix,
      int timeout,
      Duration keyDuration) {
    this.jedisPool = new JedisPool(poolConfig, host, port, timeout);
    this.keyPrefix = keyPrefix;
    this.keyDuration = keyDuration;
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      String keyPrefix,
      int timeout,
      String password) {
    this(poolConfig, host, port, keyPrefix, timeout, password, Protocol.DEFAULT_DATABASE);
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      String keyPrefix,
      int timeout,
      String password,
      int database) {
    this(poolConfig, host, port, keyPrefix, timeout, password, database, Duration.ZERO);
  }

  public RedisStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      String keyPrefix,
      int timeout,
      String password,
      int database,
      Duration keyDuration) {
    this.jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
    this.keyPrefix = keyPrefix;
    this.keyDuration = keyDuration;
  }

  @Deprecated
  public RedisStorage(Jedis jedis) {
    this(jedis, DEFAULT_PREFIX);
  }

  @Deprecated
  public RedisStorage(Jedis jedis, String keyPrefix) {
    this(
        new GenericObjectPoolConfig(),
        jedis.getClient().getHost(),
        jedis.getClient().getPort(),
        keyPrefix,
        jedis.getClient().getConnectionTimeout());
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Response<Long>> responses = new LinkedHashMap<>();

    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();

      for (AddAndGetRequest request : requests) {
        pipeline.multi();
        LimitKey limitKey = LimitKey.fromRequest(request);
        String redisKey =
            Stream.of(
                    keyPrefix,
                    limitKey.getResource(),
                    limitKey.getLimitName(),
                    limitKey.getProperty(),
                    limitKey.getBucket().toString())
                .map(RedisStorage::clean)
                .collect(Collectors.joining(KEY_SEPARATOR));

        responses.put(limitKey, pipeline.incrBy(redisKey, request.getCost()));

        // We set the expire to the given value or twice the expiration period if not defined. The expiration is there to ensure that
        // we don't fill the Redis cluster with useless keys. The actual expiration mechanism is handled by the bucketing mechanism.
        int expiration = (int) keyDuration.getSeconds();

        if (expiration == 0) {
          expiration = (int) request.getExpiration().getSeconds() * 2;
        }

        pipeline.expire(redisKey, expiration);
        pipeline.exec();
      }

      pipeline.sync();
    }

    return responses
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kvp -> kvp.getValue().get().intValue()));
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    Map<LimitKey, Integer> counters = new HashMap<>();

    try (Jedis jedis = jedisPool.getResource()) {
      Set<String> keys = jedis.keys(keyPrefix + KEY_SEPARATOR + "*");
      for (String key : keys) {
        int value = Integer.parseInt(jedis.get(key));

        String[] keyComponents = StringUtils.split(key, KEY_SEPARATOR);

        counters.put(
            new LimitKey(
                keyComponents[1],
                keyComponents[2],
                keyComponents[3],
                Instant.parse(keyComponents[4])),
            value);
      }
    }

    return counters;
  }

  @Override
  public void close() {
    jedisPool.destroy();
  }

  private static final String clean(String keyComponent) {
    return keyComponent.replace(KEY_SEPARATOR, "_");
  }
}
