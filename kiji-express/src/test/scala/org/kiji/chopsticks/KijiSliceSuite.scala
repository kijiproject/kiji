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

import org.scalatest.FunSuite
import scala.collection.immutable.List

class KijiSliceSuite extends FunSuite {
  val cell0 = Cell[Long]("info", "number", 0L, 0L)
  val cell1 = Cell[Long]("info", "number", 1L, 1L)
  val cell2 = Cell[Long]("info", "number", 2L, 2L)
  val cell3 = Cell[Long]("info", "number", 3L, 3L)
  val cell4 = Cell[Long]("info", "number", 4L, 4L)
  val cell5 = Cell[Long]("info", "number", 5L, 4L)
  val cell6 = Cell[Long]("info", "number", 6L, 6L)
  val cell7 = Cell[Long]("info", "number", 7L, 7L)
  val cellSeq : List[Cell[Long]] = List[Cell[Long]](cell7, cell6, cell5, cell4, cell3, cell2, cell1,
      cell0)

  val mapCell0 = Cell[Long]("info", "a", 0L, 0L)
  val mapCell1 = Cell[Long]("info", "a", 1L, 1L)
  val mapCell2 = Cell[Long]("info", "b", 2L, 0L)
  val mapCell3 = Cell[Long]("info", "b", 3L, 3L)
  val mapCell4 = Cell[Long]("info", "c", 4L, 0L)
  val mapCellSeq : List[Cell[Long]] = List[Cell[Long]](mapCell4, mapCell3, mapCell2, mapCell1,
      mapCell0)

  test("KijiSlice can be instantiated and maintain order.") {
    val slice: KijiSlice[Long] = new KijiSlice[Long](cellSeq)
    assert(slice.getFirst() == cell7)
    assert(slice.getLast() == cell0)
    assert(slice.cells == cellSeq)
  }

  test("KijiSlice can order by time.") {
    val slice: KijiSlice[Long] = new KijiSlice[Long](cellSeq)
    // Reverse the ordering.
    val reversedSlice: KijiSlice[Long] = slice.orderChronologically()
    assert(reversedSlice.getFirst() == cell0)
    assert(reversedSlice.getLast() == cell7)

    val reReversedSlice: KijiSlice[Long] = reversedSlice.orderReverseChronologically()
    assert(reReversedSlice.getFirst() == cell7)
    assert(reReversedSlice.getLast() == cell0)
    assert(reReversedSlice.cells == cellSeq)
  }

  test("KijiSlice can order by qualifier.") {
    val slice: KijiSlice[Long] = new KijiSlice[Long](mapCellSeq)
    // Order alphabetically, by qualifier.
    val alphabeticalSlice: KijiSlice[Long] = slice.orderByQualifier()
    assert(alphabeticalSlice.getFirst() == mapCell1)
    assert(alphabeticalSlice.getLast() == mapCell4)
  }

  test("KijiSlice can groupBy qualifier.") {
    val slice: KijiSlice[Long] = new KijiSlice[Long](mapCellSeq)
    // Group by qualifiers.
    val groupedSlices: Map[String, KijiSlice[Long]] = slice.groupByQualifier()
    assert(3 == groupedSlices.size)
    val sliceA = groupedSlices.get("a").get
    assert(2 == sliceA.size)
    val sliceB = groupedSlices.get("b").get
    assert(2 == sliceB.size)
    val sliceC = groupedSlices.get("c").get
    assert(1 == sliceC.size)
  }

  test("KijiSlice can groupBy datum.") {
    val slice: KijiSlice[Long] = new KijiSlice[Long](mapCellSeq)
    // Group by the datum contained in the cell.
    val groupedSlices: Map[Long, KijiSlice[Long]] = slice.groupBy[Long]({cell: Cell[Long] =>
      cell.datum})
    assert(3 == groupedSlices.size)
    val slice0 = groupedSlices.get(0L).get.orderReverseChronologically()
    assert(3 == slice0.size)
    assert(4L == slice0.getFirst().version)
    val slice1 = groupedSlices.get(1L).get
    assert(1 == slice1.size)
    val slice3 = groupedSlices.get(3L).get
    assert(1 == slice3.size)
  }
}
