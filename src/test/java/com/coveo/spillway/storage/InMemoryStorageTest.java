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

import org.junit.Before;
import org.junit.Test;

import com.coveo.spillway.storage.InMemoryStorage;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;

public class InMemoryStorageTest {

  private static final String RESOURCE1 = "someResource";
  private static final String LIMIT1 = "someLimit";
  private static final String PROPERTY1 = "someProperty";
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final Instant TIMESTAMP = Instant.now();

  private InMemoryStorage storage;

  @Before
  public void setup() {
    storage = new InMemoryStorage();
  }

  @Test
  public void canAddLargeValues() {
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5).getValue();
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void canAddLargeValuesToExisitingCounters() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP);
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, EXPIRATION, TIMESTAMP, 5).getValue();

    assertThat(result).isEqualTo(6);
  }

  @Test
  public void expiredEntriesAreRemovedFromDebugInfo() throws InterruptedException {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, Duration.ofSeconds(1), Instant.now());
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);
    Thread.sleep(2000);
    assertThat(storage.debugCurrentLimitCounters()).isEmpty();
  }
}
