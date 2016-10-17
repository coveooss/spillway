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
package com.coveo.spillway;

public class PercentageThresholdTrigger extends AbstractLimitTrigger {

  private final double triggerPercentage;

  public PercentageThresholdTrigger(double triggerPercentage, LimitTriggerCallback callback) {
    super(callback);

    if (triggerPercentage <= 0 || triggerPercentage >= 1) {
      throw new IllegalArgumentException("Trigger Percentage must be between 0 and 1");
    }

    this.triggerPercentage = triggerPercentage;
  }

  public double getTriggerPercentage() {
    return triggerPercentage;
  }

  @Override
  protected <T> boolean triggered(
      T context, int cost, int currentLimitValue, LimitDefinition limitDefinition) {
    int previousLimitValue = currentLimitValue - cost;
    double previousPercentage = previousLimitValue / (double) limitDefinition.getCapacity();
    double currentPercentage = currentLimitValue / (double) limitDefinition.getCapacity();

    return currentPercentage > triggerPercentage && previousPercentage <= triggerPercentage;
  }
}
