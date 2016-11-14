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
package com.coveo.spillway.storage.sliding;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;

/**
 * Implementation of {@link SlidingLimitUsageStorage} using a Redis storage.
 * <p>
 * Uses a {@link Jedis} client to communicate with the database.
 * It will automatically reconnect to the Redis server in case of connection lost.
 * <p>
 * This storage should NOT be used as a standalone sliding storage and should be
 * wrapped inside a {@link AsyncSlidingLimitUsageStorage}.
 *
 * @author Emile Fugulin
 * @since 1.1.0
 */
public class RedisSlidingStorage implements SlidingLimitUsageStorage {
  private static final String KEY_SEPARATOR = "|";
  private static final String DEFAULT_PREFIX = "spillway";

  private final JedisPool jedisPool;
  private final String keyPrefix;

  private Duration retention;
  private Duration slideSize;

  public RedisSlidingStorage(URI uri, Duration retention, Duration slideSize) {
    this(uri, retention, slideSize, DEFAULT_PREFIX);
  }

  public RedisSlidingStorage(URI uri, Duration retention, Duration slideSize, String prefix) {
    this(uri, retention, slideSize, prefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisSlidingStorage(
      URI uri, Duration retention, Duration slideSize, String prefix, int timeout) {
    this(new GenericObjectPoolConfig(), uri, retention, slideSize, prefix, timeout);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig, URI uri, Duration retention, Duration slideSize) {
    this(poolConfig, uri, retention, slideSize, DEFAULT_PREFIX);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      URI uri,
      Duration retention,
      Duration slideSize,
      String prefix) {
    this(poolConfig, uri, retention, slideSize, prefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      URI uri,
      Duration retention,
      Duration slideSize,
      String prefix,
      int timeout) {
    jedisPool = new JedisPool(poolConfig, uri, timeout);
    keyPrefix = prefix;
  }

  public RedisSlidingStorage(String host, Duration retention, Duration slideSize) {
    this(host, Protocol.DEFAULT_PORT, retention, slideSize);
  }

  public RedisSlidingStorage(String host, int port, Duration retention, Duration slideSize) {
    this(host, port, retention, slideSize, DEFAULT_PREFIX);
  }

  public RedisSlidingStorage(
      String host, int port, Duration retention, Duration slideSize, String prefix) {
    this(new GenericObjectPoolConfig(), host, port, retention, slideSize, prefix);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig, String host, Duration retention, Duration slideSize) {
    this(poolConfig, host, Protocol.DEFAULT_PORT, retention, slideSize);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      Duration retention,
      Duration slideSize) {
    this(poolConfig, host, port, retention, slideSize, DEFAULT_PREFIX);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      Duration retention,
      Duration slideSize,
      String prefix) {
    this(poolConfig, host, port, retention, slideSize, prefix, Protocol.DEFAULT_TIMEOUT);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      Duration retention,
      Duration slideSize,
      String prefix,
      int timeout) {
    jedisPool = new JedisPool(poolConfig, host, port, timeout);
    keyPrefix = prefix;
    this.retention = retention;
    this.slideSize = slideSize;
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      Duration retention,
      Duration slideSize,
      String prefix,
      int timeout,
      String password) {
    this(
        poolConfig,
        host,
        port,
        retention,
        slideSize,
        prefix,
        timeout,
        password,
        Protocol.DEFAULT_DATABASE);
  }

  public RedisSlidingStorage(
      GenericObjectPoolConfig poolConfig,
      String host,
      int port,
      Duration retention,
      Duration slideSize,
      String prefix,
      int timeout,
      String password,
      int database) {
    jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
    keyPrefix = prefix;
    this.retention = retention;
    this.slideSize = slideSize;
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
                .map(RedisSlidingStorage::clean)
                .collect(Collectors.joining(KEY_SEPARATOR));

        responses.put(limitKey, pipeline.incrBy(redisKey, request.getCost()));
        // We set the expire to twice the expiration period. The expiration is there to ensure that we don't fill the Redis cluster with
        // useless keys.
        pipeline.expire(redisKey, (int) retention.getSeconds());
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

  @Override
  public Duration getRetention() {
    return retention;
  }

  @Override
  public Duration getSlideSize() {
    return slideSize;
  }

  private static final String clean(String keyComponent) {
    return keyComponent.replace(KEY_SEPARATOR, "_");
  }
}
