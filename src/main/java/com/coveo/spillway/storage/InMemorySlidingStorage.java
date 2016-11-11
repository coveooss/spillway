package com.coveo.spillway.storage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.Capacity;

public class InMemorySlidingStorage implements LimitUsageStorage
{
  private Map<Instant, Map<LimitKey, Capacity>> map = new ConcurrentHashMap<>();
  private Clock clock = Clock.systemDefaultZone();
  private Duration retention;
  private Duration slideSize;

  public InMemorySlidingStorage(Duration retention,
                                Duration slideSize)
  {
    this.retention = retention;
    this.slideSize = slideSize;
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests)
  {
    Map<LimitKey, Integer> updatedEntries = new HashMap<>();

    for (AddAndGetRequest request : requests) {
      Instant bucket = Instant.ofEpochMilli((request.getEventTimestamp().toEpochMilli() / slideSize.toMillis())
          * slideSize.toMillis());

      LimitKey limitKey = LimitKey.fromRequest(request);

      Map<LimitKey, Capacity> mapWithThisExpiration = map.computeIfAbsent(bucket, (key) -> new HashMap<>());
      Capacity counter = mapWithThisExpiration.computeIfAbsent(limitKey, (key) -> new Capacity());
      counter.addAndGet(request.getCost());
      
      updatedEntries.put(limitKey, calculateLimitTotal(bucket, request.getExpiration(), limitKey));
    }
    removeExpiredEntries();

    return updatedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() throws Exception
  {
  }
  
  private void removeExpiredEntries()
  {
    Instant oldest = Instant.now(clock).minus(retention);
    Set<Instant> expiredEntries =
        map.keySet()
            .stream()
            .filter(bucket -> bucket.isBefore(oldest))
            .collect(Collectors.toSet());
    map.keySet().removeAll(expiredEntries);
  }

  private Integer calculateLimitTotal(Instant currentBucket,
                                      Duration limitSize,
                                      LimitKey limitKey)
  {
    Instant oldestBucket = currentBucket.minus(limitSize);

    return map.entrySet()
              .stream()
              .filter(entry -> entry.getKey().compareTo(oldestBucket) > 0)
              .flatMap(entry -> entry.getValue().entrySet().stream())
              .filter(entry -> entry.getKey().equals(limitKey))
              .map(entry -> entry.getValue().get())
              .mapToInt(Integer::intValue)
              .sum();
  }
}