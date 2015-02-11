/**
 * (c) Copyright 2015 WibiData, Inc.
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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiPartition;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.cassandra.CassandraKijiClientTest;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestCassandraKijiResultScanner extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiResultScanner.class);

  /**
   * Test that a partitioned Kiji scan correctly returns all values in the table.
   */
  @Test
  public void testScanPartitions() throws IOException {
    final int n = 100000;
    final KijiColumnName column = KijiColumnName.create("primitive", "string_column");

    final Kiji kiji = getKiji();
    kiji.createTable(
        KijiTableLayouts.getLayout("org/kiji/schema/layout/all-types-no-counters-schema.json"));

    List<KijiResult<Utf8>> results = Lists.newArrayList();

    final KijiTable table = kiji.openTable("all_types_table");
    try {
      try (KijiBufferedWriter writer = table.getWriterFactory().openBufferedWriter()) {
        for (long i = 0; i < n; i++) {
          writer.put(
              table.getEntityId(i),
              column.getFamily(),
              column.getQualifier(),
              Long.toString(i));
        }
      }

      try (KijiTableReader reader = table.openTableReader()) {
        final Collection<? extends KijiPartition> partitions = table.getPartitions();
        final KijiDataRequest dataRequest =
            KijiDataRequest.create(column.getFamily(), column.getQualifier());

        for (KijiPartition partition : partitions) {
          results.addAll(
              ImmutableList.copyOf(reader.<Utf8>getKijiResultScanner(dataRequest, partition)));
        }
      }
    } finally {
      table.release();
    }

    Assert.assertEquals(results.size(), n);

    Collections.sort(
        results, new Comparator<KijiResult<Utf8>>() {
          @Override
          public int compare(final KijiResult<Utf8> kr1, final KijiResult<Utf8> kr2) {
            Long key1 = kr1.getEntityId().getComponentByIndex(0);
            Long key2 = kr2.getEntityId().getComponentByIndex(0);
            return key1.compareTo(key2);
          }
        });

    for (int i = 0; i < 10000; i++) {
      final KijiResult<Utf8> result = results.get(i);
      Assert.assertEquals(1, Iterables.size(result));
      Assert.assertEquals(Long.valueOf(i), result.getEntityId().<Long>getComponentByIndex(0));
    }
  }
}
