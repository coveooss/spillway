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
package com.coveo.spillway.trigger;

import com.coveo.spillway.limit.LimitDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class PercentageThresholdTriggerTest {

  private LimitTriggerCallback callback;
  private final LimitDefinition limitDef =
      new LimitDefinition("testLimit", 100, Duration.ofDays(1));
  private PercentageThresholdTrigger trigger;

  @BeforeEach
  public void setup() {
    callback = mock(LimitTriggerCallback.class);
    // Will trigger at 50% of the limit
    trigger = new PercentageThresholdTrigger(0.5, callback);
  }

  @Test
  public void negativeThresholdThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> new PercentageThresholdTrigger(-1, callback));
  }

  @Test
  public void largerThanOneThresholdThrows() {
    assertThrows(IllegalArgumentException.class, () -> new PercentageThresholdTrigger(2, callback));
  }

  @Test
  public void underThresholdDoesNotTrigger() {
    assertThat(trigger.triggered(null, 10, limitDef)).isFalse();
  }

  @Test
  public void justUnderThresholdDoesNotTrigger() {
    assertThat(trigger.triggered(null, 50, limitDef)).isFalse();
  }

  @Test
  public void overThresholdTriggers() {
    assertThat(trigger.triggered(null, 51, limitDef)).isTrue();
  }
}
