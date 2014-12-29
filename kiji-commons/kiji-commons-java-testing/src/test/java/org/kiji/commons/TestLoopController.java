package org.kiji.commons;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.junit.Test;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
