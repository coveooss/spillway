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

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * Implementation of {@link LimitUsageStorage} using a Redis storage.
 * </p>
 * Uses a {@link Jedis} client to communicate with the database.
 * </p>
 * We suggest to wrap this storage in the {@link AsyncLimitUsageStorage}
 * to avoid slowing down queries if external troubles occurs with the database.
 * 
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class RedisStorage implements LimitUsageStorage {

  private static final String KEY_SEPARATOR = "|";

  private final Jedis jedis;
  private final String keyPrefix;

  public RedisStorage(String host) {
    this(new Jedis(host));
  }

  public RedisStorage(String host, int port) {
    this(new Jedis(host, port));
  }

  public RedisStorage(URI uri) {
    this(new Jedis(uri));
  }

  public RedisStorage(Jedis jedis) {
    this(jedis, "spillway");
  }

  public RedisStorage(Jedis jedis, String prefix) {
    this.jedis = jedis;
    this.keyPrefix = prefix;
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Pipeline pipeline = jedis.pipelined();

    Map<LimitKey, Response<Long>> responses = new LinkedHashMap<>();
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
      // We set the expire to twice the expiration period. The expiration is there to ensure that we don't fill the Redis cluster with
      // useless keys. The actual expiration mechanism is handled by the bucketing mechanism.
      pipeline.expire(redisKey, (int) request.getExpiration().getSeconds() * 2);
      pipeline.exec();
    }

    pipeline.sync();
    return responses
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kvp -> kvp.getValue().get().intValue()));
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    Map<LimitKey, Integer> counters = new HashMap<>();

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

    return counters;
  }

  private static final String clean(String keyComponent) {
    return keyComponent.replace(KEY_SEPARATOR, "_");
  }
}
