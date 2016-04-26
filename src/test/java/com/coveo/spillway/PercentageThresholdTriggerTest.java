package com.coveo.spillway;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

public class PercentageThresholdTriggerTest {

  private LimitTriggerCallback callback;
  private LimitDefinition limitDef = new LimitDefinition("testLimit", 100, Duration.ofDays(1));
  private PercentageThresholdTrigger trigger;

  @Before
  public void setup() {
    callback = mock(LimitTriggerCallback.class);
    // Will trigger at 50% of the limit
    trigger = new PercentageThresholdTrigger(0.5, callback);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeThresholdThrows() {
    new PercentageThresholdTrigger(-1, callback);
  }

  @Test(expected = IllegalArgumentException.class)
  public void largerThanOneThresholdThrows() {
    new PercentageThresholdTrigger(2, callback);
  }

  @Test
  public void underThresholdDoesNotTrigger() {
    assertThat(trigger.triggered(null, 1, 10, limitDef)).isFalse();
  }

  @Test
  public void justUnderThresholdDoesNotTrigger() {
    assertThat(trigger.triggered(null, 1, 50, limitDef)).isFalse();
  }

  @Test
  public void overThresholdTriggers() {
    assertThat(trigger.triggered(null, 1, 51, limitDef)).isTrue();
  }

  @Test
  public void alreadyOverThresholdDoesNotTrigger() {

    assertThat(trigger.triggered(null, 1, 52, limitDef)).isFalse();
  }
}
