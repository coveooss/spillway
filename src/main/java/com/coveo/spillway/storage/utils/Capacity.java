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
package com.coveo.spillway.storage.utils;

import java.util.concurrent.atomic.AtomicInteger;

import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.storage.InMemoryStorage;

/**
 * Container of {@link AtomicInteger}s used in the {@link InMemoryStorage}
 * to represent the current capacity of a {@link Limit}.
 *
 * @author Emile Fugulin
 * @since 1.0.0
 */
public class Capacity {
  private AtomicInteger delta = new AtomicInteger(0);
  private AtomicInteger total = new AtomicInteger(0);

  public Capacity() {
    this(0);
  }

  public Capacity(int total) {
    this.delta = new AtomicInteger(0);
    this.total = new AtomicInteger(total);
  }

  public Integer addAndGet(int cost) {
    return delta.addAndGet(cost) + total.get();
  }

  public Integer substractAndGet(int cost) {
    return delta.addAndGet(-cost) + total.get();
  }

  public Integer get() {
    return delta.get() + total.get();
  }

  public Integer getDelta() {
    return delta.get();
  }

  public void setTotal(int cost) {
    total.set(cost);
  }
}
