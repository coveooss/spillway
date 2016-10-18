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
package com.coveo.spillway.limit;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.coveo.spillway.trigger.LimitTrigger;
import com.coveo.spillway.trigger.LimitTriggerCallback;
import com.coveo.spillway.trigger.PercentageThresholdTrigger;
import com.coveo.spillway.trigger.ValueThresholdTrigger;

/**
 * Contains helper methods to easily create {@link com.coveo.spillway.limit.Limit}
 * General usage is the following :
 * <pre>
 * {@code
 * LimitBuilder.of("perIp").to(3).per(Duration.ofHours(1)).withExceededCallback(myCallback).build();
 * }
 * </pre>
 *
 * @param <T> The type of the context. String if not using a propertyExtractor
 *            ({@link LimitBuilder#of(String, java.util.function.Function)}).
 * @see Limit
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class LimitBuilder<T> {

  private String limitName;
  private Duration limitExpiration;
  private int limitCapacity;

  private Function<T, String> propertyExtractor;
  private List<LimitTrigger> triggers = new ArrayList<>();

  private LimitBuilder() {}

  public LimitBuilder<T> to(int capacity) {
    this.limitCapacity = capacity;
    return this;
  }

  /**
   * @param expiration The duration of the limit before it is reset
   * @return The current LimitBuilder
   */
  public LimitBuilder<T> per(Duration expiration) {
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
   * @return The current LimitBuilder
   */
  public LimitBuilder<T> withLimitTrigger(LimitTrigger limitTrigger) {
    this.triggers.add(limitTrigger);
    return this;
  }

  /**
   * Adds a call back that will be called when the specified limit (using {@link #to(int)}) is reached.
   *
   * @param limitTriggerCallback The callback {@link LimitTriggerCallback}
   * @return The current LimitBuilder
   */
  public LimitBuilder<T> withExceededCallback(LimitTriggerCallback limitTriggerCallback) {
    triggers.add(new ValueThresholdTrigger(limitCapacity, limitTriggerCallback));
    return this;
  }

  /**
   * When all parameters are set, call this method to get the resulting {@link Limit}
   *
   * @return The built limit
   */
  public Limit<T> build() {
    return new Limit<>(
        new LimitDefinition(limitName, limitCapacity, limitExpiration),
        propertyExtractor,
        triggers);
  }

  /**
   * Begin the creation of a new limit.
   *
   * @param limitName The name of the created limit
   * @return A new LimitBuilder
   */
  public static LimitBuilder<String> of(String limitName) {
    return of(limitName, Function.identity());
  }

  /**
   * Begin the creation of a new limit.
   *
   * @param <T> The type of the context. String if not using a propertyExtractor
   *            ({@link LimitBuilder#of(String, java.util.function.Function)}).
   *
   * @param limitName The name of the created limit
   * @param propertyExtractor Function used to fetch the throttled property
   * @return A new LimitBuilder
   */
  public static <T> LimitBuilder<T> of(String limitName, Function<T, String> propertyExtractor) {
    LimitBuilder<T> limitBuilder = new LimitBuilder<>();
    limitBuilder.limitName = limitName;
    limitBuilder.propertyExtractor = propertyExtractor;
    return limitBuilder;
  }
}
