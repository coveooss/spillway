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

import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.limit.LimitDefinition;

/**
 * Interface for all limit triggers.
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public interface LimitTrigger {
  /**
   * The method will be called each time a query is checked.
   *
   * @param <T> The type of the context. String if not using a propertyExtractor
 *              ({@link LimitBuilder#of(String, java.util.function.Function)}).
   *
   * @param context Either the name of the limit OR the object on which the propertyExtractor ({@link LimitBuilder#of(String, java.util.function.Function)})
   *                will be applied if it was specified
   * @param cost The cost of the current query
   * @param currentValue The current limit associated counter (including the current query cost)
   * @param limitDefinition The properties of the current limit
   */
  <T> void callbackIfRequired(
      T context, int cost, int currentValue, LimitDefinition limitDefinition);
}
