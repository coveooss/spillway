package com.coveo.spillway.trigger;

import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;

import com.coveo.spillway.limit.LimitDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbstractLimitTriggerTest {

  private static final LimitDefinition LIMIT_DEFINITION =
      new LimitDefinition("testLimit", 100, Duration.ofMinutes(1));

  private static class SimpleThresholdTrigger extends AbstractLimitTrigger {

    public SimpleThresholdTrigger(LimitTriggerCallback callback) {
      super(callback);
    }

    @Override
    protected <T> boolean triggered(
        T context, int currentLimitValue, LimitDefinition limitDefinition) {
      return true;
    }
  }

  private LimitTriggerCallback callback;
  private AbstractLimitTrigger abstractLimitTrigger;

  @BeforeEach
  public void setup() {
    callback = mock(LimitTriggerCallback.class);
    abstractLimitTrigger = new SimpleThresholdTrigger(callback);
  }

  @Test
  public void testCallbackNotTriggeredTwiceInSameBucket() {
    Instant now = givenABucketStartingInstant();
    abstractLimitTrigger.callbackIfRequired(null, 1, now, 1, LIMIT_DEFINITION);
    abstractLimitTrigger.callbackIfRequired(null, 1, now.plusSeconds(10), 1, LIMIT_DEFINITION);
    abstractLimitTrigger.callbackIfRequired(null, 1, now.plusSeconds(20), 1, LIMIT_DEFINITION);

    verify(callback).trigger(any(LimitDefinition.class), isNull());
  }

  @Test
  public void testCallbackTriggeredWhenBucketChange() {
    Instant now = givenABucketStartingInstant();
    abstractLimitTrigger.callbackIfRequired(null, 1, now, 1, LIMIT_DEFINITION);
    abstractLimitTrigger.callbackIfRequired(null, 1, now.plusSeconds(70), 1, LIMIT_DEFINITION);

    verify(callback, times(2)).trigger(any(LimitDefinition.class), isNull());
  }

  private Instant givenABucketStartingInstant() {
    return Instant.ofEpochMilli(
        (Instant.now().toEpochMilli() / LIMIT_DEFINITION.getExpiration().toMillis())
            * LIMIT_DEFINITION.getExpiration().toMillis());
  }
}
