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

package org.kiji.mapreduce.framework;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for various operations on SubSplits and TokenRanges.
 */
public class TestSubSplits {
  private static final Logger LOG = LoggerFactory.getLogger(TestSubSplits.class);
  private static final String HOST0 = "host0";
  private static final String HOST1 = "host1";

  @Test
  public void testCombineSubSplits() throws IOException {
    List<CassandraSubSplit> smallSubSplits = Lists.newArrayList(
        CassandraSubSplit.createFromHost(0L, 1L, HOST0),
        CassandraSubSplit.createFromHost(2L, 3L, HOST1),
        CassandraSubSplit.createFromHost(4L, 5L, HOST0),
        CassandraSubSplit.createFromHost(6L, 7L, HOST1)
    );

    CassandraSubSplitCombiner cassandraSubSplitCombiner = new CassandraSubSplitCombiner();
    List<CassandraInputSplit> inputSplits =
        cassandraSubSplitCombiner.combineSubsplits(smallSubSplits, 2);
    assertEquals(2, inputSplits.size());

    // Check that one split is for host0 and one is for host1.
    for (CassandraInputSplit inputSplit : inputSplits) {
      String[] hosts = inputSplit.getLocations();
      assertEquals(1, hosts.length);
    }
  }

  @Test
  public void testCreateSubSplits() throws IOException {
    Map<Long, String> tokensToMasterNodes = Maps.newHashMap();
    tokensToMasterNodes.put(1L, HOST0);
    tokensToMasterNodes.put(2L, HOST1);
    tokensToMasterNodes.put(3L, HOST0);
    tokensToMasterNodes.put(4L, HOST1);

    CassandraSubSplitCreator creator = new CassandraSubSplitCreator();

    List<CassandraSubSplit> subSplits = creator.createInitialSubSplits(tokensToMasterNodes);
    LOG.info(subSplits.toString());

    assertEquals(5, subSplits.size());
    assertEquals(Long.MIN_VALUE, subSplits.get(0).getStartToken());
    assertEquals(1L, subSplits.get(0).getEndToken());
    assertEquals(2L, subSplits.get(1).getStartToken());
    assertEquals(2L, subSplits.get(1).getEndToken());
    assertEquals(Long.MAX_VALUE, subSplits.get(4).getEndToken());


  }
}

