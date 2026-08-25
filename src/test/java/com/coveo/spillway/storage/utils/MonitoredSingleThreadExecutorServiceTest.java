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

import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class MonitoredSingleThreadExecutorServiceTest {

  @Test
  public void exposesWorkQueueSize() throws InterruptedException {
    MonitoredSingleThreadExecutorService executorService =
        new MonitoredSingleThreadExecutorService();
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);

    try {
      executorService.execute(
          () -> {
            workerStarted.countDown();
            try {
              releaseWorker.await();
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
            }
          });
      assertThat(workerStarted.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(executorService.getMetrics().queueSize()).isEqualTo(0);

      executorService.execute(() -> {});

      assertThat(executorService.getMetrics().queueSize()).isEqualTo(1);
    } finally {
      releaseWorker.countDown();
      executorService.shutdown();
      assertThat(executorService.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(executorService.getMetrics().queueSize()).isEqualTo(0);
  }
}
