package com.coveo.spillway;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

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
  public List<Integer> addAndGet(List<AddAndGetRequest> requests) {
    Pipeline pipeline = jedis.pipelined();

    List<Response<Long>> responses = new ArrayList<>();
    for (AddAndGetRequest request : requests) {
      pipeline.multi();
      String bucketString =
          InstantUtils.truncate(request.getEventTimestamp(), request.getExpiration()).toString();
      String key =
          Stream.of(
                  keyPrefix,
                  request.getResource(),
                  request.getLimitName(),
                  request.getProperty(),
                  bucketString)
              .map(RedisStorage::clean)
              .collect(Collectors.joining(KEY_SEPARATOR));

      responses.add(pipeline.incrBy(key, request.getIncrementBy()));
      // We set the expire to twice the expiration period. The expiration is there to ensure that we don't fill the Redis cluster with
      // useless keys. The actual expiration mechanism is handled by the bucketing done via truncate().
      pipeline.expire(key, (int) request.getExpiration().getSeconds() * 2);
      pipeline.exec();
    }

    pipeline.sync();
    return responses
        .stream()
        .map(response -> response.get().intValue())
        .collect(Collectors.toList());
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
