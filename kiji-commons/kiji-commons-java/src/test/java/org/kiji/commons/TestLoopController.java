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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import junit.framework.Assert;
import org.junit.Test;

public class TestLoopController {

  @Test
  public void cycleTest() {

    final AtomicInteger count = new AtomicInteger(0);
    final LoopController lp = LoopController.create();
    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("test-loop-controller")
            .setDaemon(true)
            .build()
    );

    final Runnable runnable = new Runnable() {
      @Override
      public void run() {
        lp.await();
        count.incrementAndGet();
      }
    };

    scheduler.scheduleAtFixedRate(runnable, 1L, 1L, TimeUnit.MILLISECONDS);
    lp.start();

    // The scheduler runnable should be frozen, so we will still get the initial value
    Assert.assertEquals(0, count.get());

    // Now we allow the scheduler to run a full cycle
    lp.advance();
    Assert.assertEquals(1, count.get());

    // Allow the scheduler to run another full cycle
    lp.advance();
    Assert.assertEquals(2, count.get());
  }
}
