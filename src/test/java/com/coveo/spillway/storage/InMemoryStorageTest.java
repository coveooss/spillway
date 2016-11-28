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
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.coveo.spillway.storage.InMemoryStorage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InMemoryStorageTest {

  private static final String RESOURCE1 = "someResource";
  private static final String LIMIT1 = "someLimit";
  private static final String PROPERTY1 = "someProperty";
  private static final Duration EXPIRATION = Duration.ofHours(1);
  private static final Instant TIMESTAMP = Instant.now();

  @Mock private Clock clock;

  @InjectMocks private InMemoryStorage storage;

  @Before
  public void setup() {
    when(clock.instant()).thenReturn(Instant.now());
  }

  @Test
  public void canAddLargeValues() {
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, true, EXPIRATION, TIMESTAMP, 5).getValue();
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void canAddLargeValuesToExisitingCounters() {
    storage.incrementAndGet(RESOURCE1, LIMIT1, PROPERTY1, true, EXPIRATION, TIMESTAMP);
    int result =
        storage.addAndGet(RESOURCE1, LIMIT1, PROPERTY1, true, EXPIRATION, TIMESTAMP, 5).getValue();

    assertThat(result).isEqualTo(6);
  }

  @Test
  public void expiredEntriesAreRemovedFromDebugInfo() {
    storage.incrementAndGet(
        RESOURCE1, LIMIT1, PROPERTY1, true, Duration.ofSeconds(2), Instant.now());
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);
    assertThat(storage.debugCurrentLimitCounters()).hasSize(1);

    // Fake sleep two seconds to ensure that we bump to another bucket
    when(clock.instant()).thenReturn(Instant.now().plusSeconds(2));

    assertThat(storage.debugCurrentLimitCounters()).isEmpty();
  }
}
