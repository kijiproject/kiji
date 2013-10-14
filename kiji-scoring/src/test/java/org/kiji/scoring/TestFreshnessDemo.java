/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.scoring;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.lib.ShelfLife;

/**
* Demo of KijiScoring freshening.
*/
public class TestFreshnessDemo extends KijiClientTest {

  private static final class DemoScoreFunction extends ScoreFunction {

    @Override
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return KijiDataRequest.create("info", "visits");
    }

    // TODO how does this overriding work?
    @Override
    public Long score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      final Long oldValue = dataToScore.getMostRecentValue("info", "visits");
      return oldValue + 1L;
    }
  }

  @Test
  public void testDemo() throws IOException {
    // Get a Kiji instance.
    final Kiji kiji = getKiji();

    // Create the "user" table.
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    // Create a ShelfLife freshness policy with a 1 day shelf life duration.
    final KijiFreshnessPolicy policy = new ShelfLife(86400000);

    final KijiColumnName column = new KijiColumnName("info", "visits");

    KijiTable table = null;
    KijiFreshnessManager manager = null;
    FreshKijiTableReader freshReader = null;
    try {
      // Get a table from the Kiji instance.
      table = kiji.openTable("user");
      // Get a KijiFreshnessManager for the Kiji instance.
      manager = KijiFreshnessManager.create(kiji);
      // Store the Freshener in the meta table for the table "user" and column "info:visits"
      // using the ShelfLife freshness policy created above and the DemoScoreFunction.
      manager.registerFreshener(
          table.getName(),
          column,
          policy,
          new DemoScoreFunction(),
          Collections.<String, String>emptyMap(),
          false,
          false);
      // Open a FreshKijiTableReader for the table with a timeout of 500 milliseconds.
      // Note: the FreshKijiTableReader must be opened after the Freshener is registered.
      freshReader = FreshKijiTableReader.Builder.create()
          .withTable(table)
          .withTimeout(500)
          .build();
      // Write an old value to the cell we plan to request with timestamp 1 and value 10.
      final EntityId eid = table.getEntityId("foo");
      final KijiTableWriter writer = table.openTableWriter();
      try {
        writer.put(eid, "info", "visits", 1L, 10L);
      } finally {
        writer.close();
      }

      // Create a data request for the desired column.
      final KijiDataRequest request = KijiDataRequest.create("info", "visits");

      // Read from the table and get back a freshened value because 1L is more than a day ago.
      assertEquals(11L, freshReader.get(eid, request).getMostRecentValue("info", "visits"));
      // Read again and get back the same value because the DemoProducer wrote a new value.
      assertEquals(11L, freshReader.get(eid, request).getMostRecentValue("info", "visits"));
    } finally {
      // Cleanup
      ResourceUtils.closeOrLog(freshReader);
      ResourceUtils.closeOrLog(manager);
      ResourceUtils.releaseOrLog(table);
      // We do not release the Kiji instance because we did not retain it.
    }
  }
}
