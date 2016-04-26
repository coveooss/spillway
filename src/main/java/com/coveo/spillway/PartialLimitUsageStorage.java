//package com.coveo.spillway;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;
//
//public class PartialLimitUsageStorage implements LimitUsageStorage {
//
//  private final LimitUsageStorage storage;
//  private final int threshold;
//
//  private InMemoryStorage cachedStorage;
//
//  public PartialLimitUsageStorage(LimitUsageStorage storage) {
//    this.storage = storage;
//    this.threshold = 10; // TODO - GSIMARD
//
//    this.cachedStorage = new InMemoryStorage();
//  }
//
//  @Override
//  public Map<LimitKey, Integer> addAndGet(Collection<AddAndGetRequest> requests) {
//
//    // TODO - GSIMARD: This doesn't work at all. If we're distributed, we need to somehow
//    // TODO - GSIMARD: resync the cache every time we pass the threshold.
//    Map<LimitKey, Integer> response = cachedStorage.addAndGet(requests);
//
//    List<AddAndGetRequest> requestsOverThreshold = requests.stream().filter(request -> response.get(LimitKey.fromRequest(request, X)) > threshold).collect(Collectors.toList());
//
//    for  (AddAndGetRequest request: requests) {
//      LimitKey requestKey = LimitKey.fromRequest(request, X);
//
//
//    }
////
////    if (newNbOfCallsSinceLastSend >= threshold) {
////      response = storage.addAndGet(requests);
////    }
//    return response;
//  }
//
//  @Override
//  public Map<LimitKey, Integer> debugCurrentLimitCounters() {
//    return storage.debugCurrentLimitCounters();
//  }
//}
