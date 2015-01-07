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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

/**
 * Test utility class for deterministically controlling the execution of cache refresh cycles. This
 * class is intended for use when the user wants to manage concurrent execution of two different
 * threads. As a sample use case, imagine we have a cached object with a scheduler that executes
 * the following runnable on a regular interval:
 *
 * int thisVal = 0;
 * LoopController lp = LoopController.create();
 * Runnable running = new Runnable {
 *   void run() {
 *     lp.await();
 *     thisVal += 1;
 *   }
 * }
 *
 * Without the LoopController inserted in the run statement, we would not be able to
 * deterministically guarantee how many cycles had completed. With the LoopController, we can now
 * advance it manually from our test code.
 *
 * Example test code:
 * lp.start(); //this advances past the first barrier and freezes the runnable at the second
 * Assert.assertEquals(0, thisVal);
 * lp.advance(); //completes a cycle of the runnable and freezes at the second barrier again
 * Assert.assertEquals(1, thisVal);
 */
public final class LoopController {

  private final CyclicBarrier mBarrierOne;
  private final CyclicBarrier mBarrierTwo;
  private final AtomicBoolean mHasFailed;
  private AtomicBoolean mInitialized;

  /**
   * Creates a LoopController and allows the user to define the number of threads the barriers will
   * await execution on.
   *
   * @param numThreads The number of different threads the barriers will await execution on.
   * @return A new LoopController.
   */
  public static LoopController create(
      final int numThreads
  ) {
    return new LoopController(numThreads);
  }

  /**
   * Creates a LoopController with barriers awaiting 2 threads.
   *
   * @return A new LoopController.
   */
  public static LoopController create() {
    return new LoopController(2);
  }

  /**
   * Private constructor.
   *
   * @param numThreads The number of different threads the barriers will await execution on.
   */
  private LoopController(
      final int numThreads
  ) {
    mBarrierOne = new CyclicBarrier(numThreads);
    mBarrierTwo = new CyclicBarrier(numThreads);
    mHasFailed = new AtomicBoolean(false);
    mInitialized = new AtomicBoolean(false);
  }

  /**
   * Advances the barriers sequentially. This should be called from the cache. This method does
   * not ensure that the barriers don't fail since it is possible these barriers may fail during
   * shutdown.
   */
  public void await() {
    try {
      mBarrierOne.await();
      mBarrierTwo.await();
    } catch (Exception e) {
      mHasFailed.set(true);
    }
  }

  /**
   * Initializes the cycle of calls that will be frozen in between the two barriers. This should
   * only be called once, or it will throw an exception.
   */
  public void start() {
    Preconditions.checkState(!mInitialized.getAndSet(true));
    try {
      mBarrierOne.await();
    } catch (Exception e) {
      mHasFailed.set(true);
    }
    Preconditions.checkState(!mHasFailed.get());
  }

  /**
   * Used to advance the LoopController a full refresh cycle, after which it will be frozen waiting
   * on the second barrier.
   */
  public void advance() {
    Preconditions.checkState(mInitialized.get());
    try {
      mBarrierTwo.await();
      mBarrierOne.await();
    } catch (Exception e) {
      mHasFailed.set(true);
    }
    Preconditions.checkState(!mHasFailed.get());
  }

  /**
   * Allows the user to advance the refresh cycle a given number of loops.
   *
   * @param numLoops The number of loops to advance.
   */
  public void advance(
      final int numLoops
  ) {
    for (int i=0; i < numLoops; i++) {
      advance();
    }
  }

  /**
   * Returns true if the barriers encountered errors while testing.
   *
   * @return True if the barriers encountered errors in the execution cycle.
   */
  public boolean hasFailed() {
    return mHasFailed.get();
  }

}
