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
 * Functional interface for all call-back in the library.
 * </p>
 * This function is used by {@link AbstractLimitTrigger}.
 * 
 * @see AbstractLimitTrigger
 * 
 * @author Guillaume Simard
 * @since 1.0.0
 */
@FunctionalInterface
public interface LimitTriggerCallback {
  LimitTriggerCallback DO_NOTHING = (limitDefinition, context) -> {};

  /**
   * This method is called by by {@link AbstractLimitTrigger#callbackIfRequired(Object, int, int, LimitDefinition)}
   * if the current limit associated counter meets the trigger value.
   * 
   * @param definition The properties of the current limit
   * @param context Either the name of the limit OR the object on which the propertyExtractor 
   *                ({@link LimitBuilder#of(String, java.util.function.Function)}) 
   *                has been applied if it was specified
   */
  void trigger(LimitDefinition definition, Object context);

  static LimitTriggerCallback doNothing() {
    return DO_NOTHING;
  }
}
