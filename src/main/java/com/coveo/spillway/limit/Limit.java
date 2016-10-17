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
import java.util.List;
import java.util.function.Function;

import com.coveo.spillway.trigger.LimitTrigger;

/**
 * Contains all the the necessary information to enforce a limit
 * specified by the user. This also incluses the various call back 
 * when the capacity is reached.
 * </p>
 * Should always be created using the {@link LimitBuilder}.
 * 
 * @see LimitBuilder
 * 
 * @author Guillaume Simard
 * @since 1.0.0
 */
public class Limit<T> {

  private LimitDefinition definition;
  private Function<T, String> propertyExtractor;

  private List<LimitTrigger> limitTriggers;

  /*package*/ Limit(
      LimitDefinition definition,
      Function<T, String> propertyExtractor,
      List<LimitTrigger> limitTriggers) {
    this.definition = definition;
    this.propertyExtractor = propertyExtractor;
    this.limitTriggers = limitTriggers;
  }

  public LimitDefinition getDefinition() {
    return definition;
  }

  public List<LimitTrigger> getLimitTriggers() {
    return limitTriggers;
  }

  public String getProperty(T context) {
    return propertyExtractor.apply(context);
  }

  public String getName() {
    return definition.getName();
  }

  public Duration getExpiration() {
    return definition.getExpiration();
  }

  public int getCapacity() {
    return definition.getCapacity();
  }

  @Override
  public String toString() {
    return definition.toString();
  }
}
