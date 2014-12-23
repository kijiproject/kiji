/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.commons;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRefreshingReference {

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshingReference.class);

  private class BarrierLoader implements RefreshingLoader<Integer> {

    private final CyclicBarrier mFirstBarrier = new CyclicBarrier(2);
    private final CyclicBarrier mSecondBarrier = new CyclicBarrier(2);
    private final AtomicBoolean mFailureIndicator = new AtomicBoolean(false);
    private final AtomicInteger mValue = new AtomicInteger(0);

    public CyclicBarrier getFirstBarrier() {
      return mFirstBarrier;
    }

    public CyclicBarrier getSecondBarrier() {
      return mSecondBarrier;
    }

    public void setFailureIndicator(boolean b) {
      mFailureIndicator.set(b);
    }

    public boolean getFailureIndicator() {
      return mFailureIndicator.get();
    }

    public void start() {
      try {
        mFirstBarrier.await();
      } catch (Exception e) {
        mFailureIndicator.set(true);
      }
    }

    public void advance() {
      try {
        mSecondBarrier.await();
        mFirstBarrier.await();
      } catch (Exception e) {
        mFailureIndicator.set(true);
      }
    }

    @Override
    public Integer initial() {
      return mValue.get();
    }

    @Override
    public Integer refresh(final Integer previous) {
      try {
        mFirstBarrier.await();
        mSecondBarrier.await();
      } catch (Exception e) {
        mFailureIndicator.set(true);
      }
      return mValue.get();
    }

    @Override
    public void close() { }

    public void incrementValue() {
      mValue.incrementAndGet();
    }

    public Integer getValue() {
      return mValue.get();
    }

  }

  @Test
  public void testRefresh() throws Exception {

    final BarrierLoader loader = new BarrierLoader();

    final RefreshingReference<Integer> reference =
        RefreshingReference.create(1L, TimeUnit.MILLISECONDS, loader);

    // Freezes the refresh cycle, so the cache should still contain the initial value.
    loader.start();
    Assert.assertEquals(0, reference.get().intValue());
    // Increment the value to be cached to 1
    loader.incrementValue();
    // Since the refresh is frozen by the barrier, assert that the cached value hasn't refreshed.
    Assert.assertEquals(0, reference.get().intValue());
    // Release the barrier and force a full refresh cycle.
    loader.advance();
    // After one full refresh cycle, the cached value should be 1.
    Assert.assertEquals(1, reference.get().intValue());
    // Increment the value to be cached to 2
    loader.incrementValue();
    // Force one more refresh cycle
    loader.advance();
    // After the second full refresh cycle, the cached value should be 2.
    Assert.assertEquals(2, reference.get().intValue());

    // Make sure the barrier didn't throw any exceptions during the refresh cycle
    Assert.assertFalse(loader.getFailureIndicator());

  }

  /**
   * This test verifies that the cache still works if refresh fails.
   *
   * @throws Exception Intentionally when AtomicInteger value is odd
   */
  @Test
  public void testRunException() throws Exception {

    final BarrierLoader loader = new BarrierLoader() {

      @Override
      public Integer refresh(Integer previous) {

        try {
          getFirstBarrier().await();
          getSecondBarrier().await();
        } catch (Exception e) {
          setFailureIndicator(true);
        }
        Integer currentValue = getValue();
        if (currentValue % 2 == 0) {
          return currentValue;
        } else {
          throw new RuntimeException("Value is odd, intentionally thrown");
        }
      }
    };

    RefreshingReference<Integer> cache =
        RefreshingReference.create(1L, TimeUnit.MILLISECONDS, loader);

    Assert.assertEquals(0, cache.get().intValue());
    loader.start();
    loader.incrementValue(); // Sets the value to 1
    loader.advance(); // Attempt to refresh the cache

    // Refresh will fail, so the cached value will not be updated.
    Assert.assertEquals(0, cache.get().intValue());

    loader.incrementValue(); // Sets the value to 2
    loader.advance(); // Attempt to refresh the cache
    Assert.assertEquals(2, cache.get().intValue()); // The refresh will succeed

    // Make sure the barrier didn't throw an exception while refreshing
    Assert.assertFalse(loader.getFailureIndicator());
  }

  @Test
  public void testClose() throws IOException {

    final AtomicInteger value = new AtomicInteger(0);

    RefreshingLoader<Integer> refresh = new RefreshingLoader<Integer>() {
      @Override
      public Integer initial() {
        return value.get();
      }

      @Override
      public Integer refresh(Integer previous) {
        return value.get();
      }

      @Override
      public void close() throws IOException {
        value.incrementAndGet();
      }
    };

    RefreshingReference<Integer> cache =
        RefreshingReference.create(1000L, TimeUnit.MILLISECONDS, refresh);

    Assert.assertEquals(0, value.get()); // Value should remain 0 until close call

    cache.close();

    Assert.assertEquals(1, value.get()); // Value should be incremented after close call

  }

  /**
   * The goal of this test is to ensure that after the cache is closed, the scheduler is correctly
   * shutdown and the cached value is no longer refreshing. There is no way to do this
   * deterministically, so in lieu of a deterministic test we create a cache with a very short
   * refresh period that increments a stored value every refresh cycle. We then call close and
   * wait long enough to check the cached value such that the probability that the cache is still
   * refreshing is vanishingly small.
   *
   * @throws IOException If the cache fails to close.
   */
  @Test
  public void testCloseStopsRefreshing()
      throws IOException, BrokenBarrierException, InterruptedException {

    final AtomicInteger value = new AtomicInteger(0);

    RefreshingLoader<Integer> refresh = new RefreshingLoader<Integer>() {
      @Override
      public Integer initial() {
        return value.get();
      }

      @Override
      public Integer refresh(Integer previous) {
        return value.incrementAndGet();
      }

      @Override
      public void close() throws IOException { }
    };

    RefreshingReference<Integer> cache =
        RefreshingReference.create(1L, TimeUnit.MILLISECONDS, refresh);

    // Now we close the cache and take the final value.
    cache.close();
    final Integer finalValue = cache.get();

    // Now we force this thread to sleep. If refresh were still occurring, it would be running
    // in the background.
    Thread.sleep(2000L);

    // Finally, we verify that the cache has not actually been refreshing and that the value was
    // frozen after calling close.
    Assert.assertEquals(finalValue, cache.get());
  }

}

