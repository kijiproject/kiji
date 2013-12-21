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

package org.kiji.express.flow

import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite

@RunWith(classOf[JUnitRunner])
class FlowCellSuite extends KijiSuite {
  test("FlowCells can be grouped by qualifier") {

    val qualifiers = List("", "a", "a", "a", "b", "c", "c", "d", "d", "d", "e", "f", "g")
    val versionValues = 1 to 1000

    val cells = qualifiers.zip(versionValues).map(t => FlowCell("family", t._1, t._2, t._2))
    val cellGroups = cells.groupBy(_.qualifier)

    Assert.assertEquals(cellGroups, FlowCell.groupByQualifier(cells).toMap)
  }

  test("many FlowCells can be grouped by qualifier.") {
    // Idea here is to create a bunch of groups of different sizes.
    val cells = (for {
      i <- 1 to 1000
      j <- 1 until i if i % j == 0
    } yield i).sorted.map(n => FlowCell("fam", n.toString, n, n))

    Assert.assertEquals(cells.groupBy(_.qualifier), FlowCell.groupByQualifier(cells).toMap)
  }

  test("An empty seq of FlowCells can be grouped by qualifier") {
    val cells: Seq[FlowCell[Int]] = Seq()
    Assert.assertTrue(FlowCell.groupByQualifier(cells).isEmpty)
  }
}
