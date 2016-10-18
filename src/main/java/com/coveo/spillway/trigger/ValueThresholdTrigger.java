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

/**
 * {@link AbstractLimitTrigger} that compares using a precise trigger value.
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class ValueThresholdTrigger extends AbstractLimitTrigger {
  private int triggerValue;

  public ValueThresholdTrigger(int triggerValue, LimitTriggerCallback callback) {
    super(callback);
    this.triggerValue = triggerValue;
  }

  public int getTriggerValue() {
    return triggerValue;
  }

  /**
   * This method compares the current value with the trigger value.
   * <p>
   * {@inheritDoc}
   * @return True is its the first time the value is over the trigger, false otherwise
   */
  @Override
  protected <T> boolean triggered(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition) {
    // Detect if the limit was exceeded by this call() invocation
    // This can be detected if the new value is higher than the limit and the previous value is lower
    // This is possible since the storage guarantees atomicity of operations
    int previousLimitValue = currentLimitValue - cost;
    return currentLimitValue > triggerValue && previousLimitValue <= triggerValue;
  }

  @Override
  public String toString() {
    return "LimitTrigger{" + "limitValue=" + triggerValue + '}';
  }
}
