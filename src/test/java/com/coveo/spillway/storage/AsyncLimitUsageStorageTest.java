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

import com.coveo.spillway.limit.LimitKey;
import com.coveo.spillway.storage.AsyncLimitUsageStorage;
import com.coveo.spillway.storage.utils.AddAndGetRequest;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncLimitUsageStorageTest {

  private static final Logger logger = LoggerFactory.getLogger(AsyncLimitUsageStorageTest.class);

  private static final String RESOURCE = "TheResource";
  private static final String PROPERTY = "TheProperty";
  private static final String LIMITNAME = "TheLimit";
  private static final Instant INSTANT = Instant.now();
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final int MOCKED_STORAGE_COUNTER = 100;
  private static final int MOCKED_STORAGE_SLEEP = 100;

  private AsyncLimitUsageStorage asyncStorage;
  private LimitUsageStorage mockedStorage;
  private AddAndGetRequest request =
      new AddAndGetRequest.Builder()
          .withResource(RESOURCE)
          .withProperty(PROPERTY)
          .withLimitName(LIMITNAME)
          .withEventTimestamp(INSTANT)
          .withCost(1)
          .withLimitDuration(EXPIRATION)
          .build();

  @Before
  public void setup() {
    mockedStorage = mock(LimitUsageStorage.class);
    when(mockedStorage.addAndGet(anyCollectionOf(AddAndGetRequest.class)))
        .then(
            invocation -> {
              Thread.sleep(MOCKED_STORAGE_SLEEP);
              logger.info("Mocked storage returning!");
              return ImmutableMap.of(LimitKey.fromRequest(request), MOCKED_STORAGE_COUNTER);
            });

    asyncStorage = new AsyncLimitUsageStorage(mockedStorage);
  }

  @Test
  public void canAddAndGet() throws InterruptedException {
    int counter = asyncStorage.addAndGet(request).getValue();

    assertThat(counter).isEqualTo(1);
    assertThat(asyncStorage.debugCurrentLimitCounters()).hasSize(0);
    Thread.sleep(MOCKED_STORAGE_SLEEP * 2);
    // After a while, the mockedStorage returns and the counter is overwritten.
    counter = asyncStorage.addAndGet(request).getValue();
    assertThat(counter).isEqualTo(MOCKED_STORAGE_COUNTER + 1);
  }
}
