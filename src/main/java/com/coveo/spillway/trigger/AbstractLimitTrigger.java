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

import java.time.Instant;

import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.limit.LimitDefinition;
import com.coveo.spillway.limit.utils.LimitUtils;

/**
 * Base abstract class for our triggers that implements {@link LimitTrigger}.
 * Calls a {@link LimitTriggerCallback} when the limit is reached.
 *
 * @see LimitTrigger
 * @see LimitTriggerCallback
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public abstract class AbstractLimitTrigger implements LimitTrigger {

  private final LimitTriggerCallback callback;
  private Instant triggeredBucket = Instant.EPOCH;

  public AbstractLimitTrigger(LimitTriggerCallback callback) {
    this.callback = callback;
  }

  /**
   * This method is called by {@link #callbackIfRequired(Object, int, int, LimitDefinition)} to
   * verify if the call-back you should be called.
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor
   *                ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the current query
   * @param currentLimitValue The current limit associated counter (including the current query cost)
   * @param limitDefinition The properties of the current limit
   * @return True if the limit is triggered, false otherwise
   */
  protected abstract <T> boolean triggered(
      T context, int currentLimitValue, LimitDefinition limitDefinition);

  @Override
  public <T> void callbackIfRequired(
      T context,
      int cost,
      Instant timestamp,
      int currentLimitValue,
      LimitDefinition limitDefinition) {
    Instant currentBucket = LimitUtils.calculateBucket(timestamp, limitDefinition.getExpiration());

    if (triggered(context, currentLimitValue, limitDefinition)
        && currentBucket.isAfter(triggeredBucket)) {
      triggeredBucket = currentBucket;
      callback.trigger(limitDefinition, context);
    }
  }
}