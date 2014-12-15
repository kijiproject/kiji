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

package org.kiji.commons.monitoring;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLatencyUtilsReservoir {
  private static final Logger LOG = LoggerFactory.getLogger(TestLatencyUtilsReservoir.class);

  @Test
  public void testLatencyUtilsReservoir() throws Exception {
    final Reservoir reservoir = LatencyUtilsReservoir.create();
    Assert.assertEquals(0, reservoir.getSnapshot().size());

    // LatencyStats is accurate (by default) to 2 significant digits
    reservoir.update(NANOSECONDS.convert(10, MILLISECONDS));
    reservoir.update(NANOSECONDS.convert(72, MILLISECONDS));
    reservoir.update(NANOSECONDS.convert(98, MILLISECONDS));
    final Snapshot snapshot = reservoir.getSnapshot();
    Assert.assertEquals(3, snapshot.size());

    Assert.assertEquals(
        NANOSECONDS.convert((10 + 72 + 98) / 3, MILLISECONDS),
        snapshot.getMean(),
        NANOSECONDS.convert(1, MILLISECONDS));

    Assert.assertEquals(
        NANOSECONDS.convert(72, MILLISECONDS),
        snapshot.getMedian(),
        NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));

    Assert.assertEquals(
        NANOSECONDS.convert(10, MILLISECONDS),
        snapshot.getValue(0.00),
        NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));

    Assert.assertEquals(
        NANOSECONDS.convert(98, MILLISECONDS),
        snapshot.getValue(1.00),
        NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));

    // the reservoir resets after every snapshot
    Assert.assertEquals(0, reservoir.getSnapshot().size());
  }
}
