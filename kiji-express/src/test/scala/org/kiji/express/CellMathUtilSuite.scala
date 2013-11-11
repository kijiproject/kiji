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

package org.kiji.express

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import org.kiji.express.util.CellMathUtil

@RunWith(classOf[JUnitRunner])
class CellMathUtilSuite
    extends FunSuite
    with ShouldMatchers {
  val cell0 = Cell[Long]("info", "number", 0L, 0L)
  val cell1 = Cell[Long]("info", "number", 1L, 1L)
  val cell2 = Cell[Long]("info", "number", 2L, 2L)
  val cell3 = Cell[Long]("info", "number", 3L, 3L)
  val cell4 = Cell[Long]("info", "number", 4L, 4L)
  val cell5 = Cell[Long]("info", "number", 5L, 4L)
  val cell6 = Cell[Long]("info", "number", 6L, 6L)
  val cell7 = Cell[Long]("info", "number", 7L, 7L)
  val cellSeq : List[Cell[Long]] = List(
      cell7,
      cell6,
      cell5,
      cell4,
      cell3,
      cell2,
      cell1,
      cell0
  )

  val mapCell0 = Cell[Long]("info", "a", 0L, 0L)
  val mapCell1 = Cell[Long]("info", "a", 1L, 1L)
  val mapCell2 = Cell[Long]("info", "b", 2L, 0L)
  val mapCell3 = Cell[Long]("info", "b", 3L, 3L)
  val mapCell4 = Cell[Long]("info", "c", 4L, 0L)
  val mapCellSeq : List[Cell[Long]] = List(
      mapCell4,
      mapCell3,
      mapCell2,
      mapCell1,
      mapCell0
  )

  test("KijiSlice should properly sum simple types") {
    val slice: Seq[Cell[Long]]  =  mapCellSeq.toStream
    assert(4 == CellMathUtil.sum(slice))
  }

  test("KijiSlice should properly compute the squared sum of simple types") {
    val slice: Seq[Cell[Long]]  =  mapCellSeq.toStream
    assert(10.0 == CellMathUtil.sumSquares(slice))
  }

  test("KijiSlice should properly compute the average of simple types") {
    val slice:Seq[Cell[Long]] =  mapCellSeq.toStream
   assert(.8 == CellMathUtil.mean(slice))
  }

  test("KijiSlice should properly compute the standard deviation of simple types") {
    val slice: Seq[Cell[Long]] =  mapCellSeq.toStream
    CellMathUtil.stddev(slice) should be (1.16619 plusOrMinus 0.1)
  }

  test("KijiSlice should properly compute the variance of simple types") {
    val slice: Seq[Cell[Long]] = mapCellSeq.toStream
    CellMathUtil.variance(slice) should be (1.36 plusOrMinus 0.1)
  }

  test("KijiSlice should properly find the minimum of simple types") {
    val slice: Seq[Cell[Long]] = mapCellSeq.toStream
    assert(0.0 == CellMathUtil.min(slice))
  }

  test("KijiSlice should properly find the maximum of simple types") {
    val slice: Seq[Cell[Long]] = mapCellSeq.toStream
    assert(3.0 == CellMathUtil.max(slice))
  }
}
