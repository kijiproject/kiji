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

package org.kiji.chopsticks

import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.scalatest.FunSuite

class KijiTableSplitSuite extends FunSuite {
  val hTableSplit =
    new TableSplit("name".getBytes, "startrow".getBytes, "endrow".getBytes, "location")
  val kTableSplit = new KijiTableSplit(hTableSplit)

  test("KijiTableSplit should have the same startrow as the hTableSplit.") {
    assert(hTableSplit.getStartRow.toSeq == kTableSplit.getStartRow.toSeq)
  }

  test("KijiTableSplit should have the same endrow as the hTableSplit.") {
    assert(hTableSplit.getEndRow.toSeq ==  kTableSplit.getEndRow.toSeq)
  }

  test("KijiTableSplit should have the same locations as the hTableSplit.") {
    assert(hTableSplit.getLocations.toSeq == kTableSplit.getLocations.toSeq)
  }
}
