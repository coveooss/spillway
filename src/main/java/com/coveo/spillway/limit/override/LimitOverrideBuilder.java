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
package com.coveo.spillway.limit.override;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.coveo.spillway.trigger.LimitTrigger;
import com.coveo.spillway.trigger.LimitTriggerCallback;
import com.coveo.spillway.trigger.PercentageThresholdTrigger;
import com.coveo.spillway.trigger.ValueThresholdTrigger;

/**
 * Contains helper methods to easily create a {@link LimitOverride}
 * General usage is the following :
 * <pre>
 * {@code
 * LimitOverrideBuilder.of("127.0.0.1").to(4).per(Duration.ofHours(2)).withExceededCallback(myCallback).build();
 * }
 * </pre>
 *
 * @see LimitOverride
 *
 * @author Emile Fugulin
 * @since 1.0.0
 */
public class LimitOverrideBuilder {

  private String limitProperty;
  private Duration limitExpiration;
  private int limitCapacity;

  private List<LimitTrigger> triggers = new ArrayList<>();

  private LimitOverrideBuilder() {}

  /**
   * @param capacity The overriden limit capacity before it starts throttling
   * @return The current {@link LimitOverrideBuilder}
   */
  public LimitOverrideBuilder to(int capacity) {
    this.limitCapacity = capacity;
    return this;
  }

  /**
   * @param expiration The overriden duration of the limit before it is reset
   * @return The current {@link LimitOverrideBuilder}
   */
  public LimitOverrideBuilder per(Duration expiration) {
    this.limitExpiration = expiration;
    return this;
  }

  /**
   * If necessary, adds a custom {@link LimitTrigger}.
   * Some implementations already exists.
   *
   * @see ValueThresholdTrigger
   * @see PercentageThresholdTrigger
   *
   * @param limitTrigger Custom {@link LimitTrigger}
   * @return The current {@link LimitOverrideBuilder}
   */
  public LimitOverrideBuilder withLimitTrigger(LimitTrigger limitTrigger) {
    this.triggers.add(limitTrigger);
    return this;
  }

  /**
   * Adds a call back that will be called when the specified limit override (using {@link #to(int)}) is reached.
   *
   * @param limitTriggerCallback The callback {@link LimitTriggerCallback}
   * @return The current LimitBuilder
   */
  public LimitOverrideBuilder withExceededCallback(LimitTriggerCallback limitTriggerCallback) {
    triggers.add(new ValueThresholdTrigger(limitCapacity, limitTriggerCallback));
    return this;
  }

  /**
   * When all parameters are set, call this method to get the resulting {@link LimitOverride}
   *
   * @return The built {@link LimitOverride}
   */
  public LimitOverride build() {
    return new LimitOverride(
        new LimitOverrideDefinition(limitProperty, limitCapacity, limitExpiration), triggers);
  }

  /**
   * Begin the creation of a new limit override.
   *
   * @param limitProperty The name of the overriden property
   * @return A new {@link LimitOverrideBuilder}
   */
  public static LimitOverrideBuilder of(String limitProperty) {
    LimitOverrideBuilder limitBuilder = new LimitOverrideBuilder();
    limitBuilder.limitProperty = limitProperty;
    return limitBuilder;
  }
}
