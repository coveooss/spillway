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
import java.util.Optional;
import java.util.function.Function;

import com.coveo.spillway.limit.override.LimitOverride;
import com.coveo.spillway.trigger.LimitTrigger;

/**
 * Contains all the the necessary information to enforce a limit
 * specified by the user. This also incluses the various call back
 * when the capacity is reached.
 * <p>
 * Should always be created using the {@link LimitBuilder}.
 *
 * @param <T> The type of the context. String if not using a propertyExtractor
 *            ({@link LimitBuilder#of(String, java.util.function.Function)}).
 * @see LimitBuilder
 *
 * @author Guillaume Simard
 * @author Emile Fugulin
 * @since 1.0.0
 */
public class Limit<T> {

  private LimitDefinition definition;
  private Function<T, String> propertyExtractor;
  private List<LimitOverride> limitOverrides;

  private List<LimitTrigger> limitTriggers;

  /*package*/ Limit(
      LimitDefinition definition,
      Function<T, String> propertyExtractor,
      List<LimitOverride> limitOverrides,
      List<LimitTrigger> limitTriggers) {
    this.definition = definition;
    this.propertyExtractor = propertyExtractor;
    this.limitOverrides = limitOverrides;
    this.limitTriggers = limitTriggers;
  }

  public LimitDefinition getDefinition() {
    return definition;
  }

  public LimitDefinition getDefinition(T context) {
    return findLimitOverride(context)
        .map(p -> new LimitDefinition(getName(), p.getCapacity(), p.getExpiration()))
        .orElse(getDefinition());
  }

  public List<LimitTrigger> getLimitTriggers() {
    return limitTriggers;
  }

  public List<LimitTrigger> getLimitTriggers(T context) {
    return findLimitOverride(context).map(p -> p.getLimitTriggers()).orElse(getLimitTriggers());
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

  public Duration getExpiration(T context) {
    return findLimitOverride(context).map(p -> p.getExpiration()).orElse(getExpiration());
  }

  public int getCapacity() {
    return definition.getCapacity();
  }

  public int getCapacity(T context) {
    return findLimitOverride(context).map(p -> p.getCapacity()).orElse(getCapacity());
  }

  public List<LimitOverride> getLimitOverrides() {
    return limitOverrides;
  }

  @Override
  public String toString() {
    return definition.toString();
  }

  private Optional<LimitOverride> findLimitOverride(T context) {
    String property = getProperty(context);

    for (LimitOverride limitOverride : limitOverrides) {
      if (limitOverride.getProperty().equals(property)) {
        return Optional.of(limitOverride);
      }
    }

    return Optional.empty();
  }
}
