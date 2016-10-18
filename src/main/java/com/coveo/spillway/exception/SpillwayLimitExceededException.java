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
package com.coveo.spillway.exception;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.coveo.spillway.Spillway;
import com.coveo.spillway.limit.LimitDefinition;

/**
 * Exception thrown by {@link Spillway#call(Object, int)} when the counter exceeds the limit.
 *
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class SpillwayLimitExceededException extends SpillwayException {

  private static final long serialVersionUID = 6459670418763015179L;

  private List<LimitDefinition> exceededLimits = new ArrayList<>();
  private Object context;

  public SpillwayLimitExceededException(LimitDefinition limitDefinition, Object context, int cost) {
    this(Arrays.asList(limitDefinition), context, cost);
  }

  public SpillwayLimitExceededException(
      List<LimitDefinition> limitDefinitions, Object context, int cost) {
    super(
        "Attempted to use " + cost + " units in limit " + limitDefinitions + " but it exceeds it.");
    exceededLimits.addAll(limitDefinitions);
    this.context = context;
  }

  public List<LimitDefinition> getExceededLimits() {
    return Collections.unmodifiableList(exceededLimits);
  }

  public Object getContext() {
    return context;
  }
}
