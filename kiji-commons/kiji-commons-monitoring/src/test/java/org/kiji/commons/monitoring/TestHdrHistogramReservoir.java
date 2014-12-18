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

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHdrHistogramReservoir {
  private static final Logger LOG = LoggerFactory.getLogger(TestHdrHistogramReservoir.class);

  @Test
  public void testLatencyUtilsReservoir() throws Exception {
    final Reservoir reservoir = HdrHistogramReservoir.create(1, 1000, 2);
    Assert.assertEquals(0, reservoir.getSnapshot().size());

    reservoir.update(10);
    reservoir.update(72);
    reservoir.update(98);
    final Snapshot snapshot = reservoir.getSnapshot();
    Assert.assertEquals(3, snapshot.size());

    Assert.assertEquals(((10 + 72 + 98) / 3), snapshot.getMean(), 1);

    Assert.assertEquals(72, snapshot.getMedian(), 1);

    Assert.assertEquals(10, snapshot.getValue(0.00), 1);

    Assert.assertEquals(98, snapshot.getValue(1.00), 1);

    // the reservoir resets after every snapshot
    Assert.assertEquals(0, reservoir.getSnapshot().size());
  }
}
