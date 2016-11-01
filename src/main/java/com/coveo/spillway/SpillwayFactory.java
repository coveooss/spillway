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

import java.time.Clock;

import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.storage.LimitUsageStorage;

/**
 * Factory to create {@link Spillway} objects using the specified storage method.
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class SpillwayFactory {
  private final LimitUsageStorage storage;
  private final Clock clock;

  public SpillwayFactory(LimitUsageStorage storage) {
    this.storage = storage;
    this.clock = Clock.systemDefaultZone();
  }

  public SpillwayFactory(LimitUsageStorage storage, Clock clock) {
    this.storage = storage;
    this.clock = clock;
  }

  /**
   * Creates a new {@link Spillway}
   *
   * @param <T> The type of the context. String if not using a propertyExtractor
   *            ({@link LimitBuilder#of(String, java.util.function.Function)}).
   *
   * @param resource The name of the resource on which the limit are enforced
   * @param limits The different enforced limits
   * @return The new {@link Spillway}
   */
  @SafeVarargs
  public final <T> Spillway<T> enforce(String resource, Limit<T>... limits) {
    return new Spillway<>(clock, storage, resource, limits);
  }
}
