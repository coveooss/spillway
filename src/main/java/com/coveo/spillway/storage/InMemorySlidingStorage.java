package com.coveo.spillway.storage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.coveo.spillway.storage.utils.Capacity;

/**
 * Implementation of {@link LimitUsageStorage} using memory.
 * <p>
 * Not recommended as a distributed storage solution since sharing memory
 * can be complicated. Perfect for local usages.
 * <p>
 * The difference with {@link InMemoryStorage} is that instead of fixed time
 * buckets, this implementation uses a sliding window of time and counts
 * the number of requests made in that window.
 *
 * @author Emile Fugulin
 * @since 1.1.0
 */
public class InMemorySlidingStorage implements LimitUsageStorage {
  private Map<Instant, Map<LimitKey, Capacity>> map = new ConcurrentHashMap<>();
  private Clock clock = Clock.systemDefaultZone();
  private Duration retention;
  private Duration slideSize;

  /**
   * Constructor for the {@link InMemorySlidingStorage}.
   *
   * @param retention The retention duration should be greater than highest limit duration, but
   *                  beware that the higher the retention is, the higher the memory usage will be.
   * @param slideSize The slide size determines the precision of the count, but beware that a smaller
   *                  slide size means more memory and more network traffic to sync the limits if needed.
   */
  public InMemorySlidingStorage(Duration retention, Duration slideSize) {
    this.retention = retention;
    this.slideSize = slideSize;
  }

  @Override
  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
    Map<LimitKey, Integer> updatedEntries = new LinkedHashMap<>();

    for (AddAndGetRequest request : requests) {
      Instant bucket =
          Instant.ofEpochMilli(
              (request.getEventTimestamp().toEpochMilli() / slideSize.toMillis())
                  * slideSize.toMillis());

      LimitKey limitKey = LimitKey.fromRequest(request);

      Map<LimitKey, Capacity> mapWithThisExpiration =
          map.computeIfAbsent(bucket, (key) -> new HashMap<>());
      Capacity counter = mapWithThisExpiration.computeIfAbsent(limitKey, (key) -> new Capacity());
      counter.addAndGet(request.getCost());

      updatedEntries.put(limitKey, calculateLimitTotal(bucket, request.getExpiration(), limitKey));
    }
    removeExpiredEntries();

    return updatedEntries;
  }

  @Override
  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
    removeExpiredEntries();
    return map.values()
        .stream()
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, kvp -> kvp.getValue().get()));
  }

  @Override
  public void close() throws Exception {}

  private void removeExpiredEntries() {
    Instant oldest = Instant.now(clock).minus(retention);
    Set<Instant> expiredEntries =
        map.keySet().stream().filter(bucket -> bucket.isBefore(oldest)).collect(Collectors.toSet());
    map.keySet().removeAll(expiredEntries);
  }

  private Integer calculateLimitTotal(
      Instant currentBucket, Duration limitSize, LimitKey limitKey) {
    Instant oldestBucket = currentBucket.minus(limitSize);

    return map.entrySet()
        .stream()
        .filter(entry -> entry.getKey().compareTo(oldestBucket) > 0)
        .flatMap(entry -> entry.getValue().entrySet().stream())
        .filter(entry -> limitKeysEquals(entry.getKey(), limitKey))
        .peek(
            entry2
                -> System.out.println(entry2.getKey().toString() + " : " + entry2.getValue().get()))
        .map(entry -> entry.getValue().get())
        .mapToInt(Integer::intValue)
        .sum();
  }

  // Necessary since the equals method of the LimitKey checks the bucket and we don't care about it
  private boolean limitKeysEquals(LimitKey first, LimitKey second) {
    return first.getResource() == second.getResource()
        && first.getLimitName() == second.getLimitName()
        && first.getProperty() == second.getProperty();
  }
}
