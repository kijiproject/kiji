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

import org.scalatest.FunSuite

import org.kiji.express.DSL._
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.RegexQualifierColumnFilter

class DSLSuite extends FunSuite {
  val tableURI = "kiji://.env/default/table"

  test("DSL should not let you create a grouptype column without a qualifier.") {
    intercept[KijiInvalidNameException] {
      val colReq: ColumnRequest = Column("search")
    }
  }

  test("DSL should not let you create a maptype column request with a qualifier in the col name.") {
    intercept[KijiInvalidNameException] {
      val colReq: ColumnRequest = MapFamily("info:word")
    }

    intercept[KijiInvalidNameException] {
      val colReq: ColumnRequest = MapFamily("info:word")('qualifierField)
    }
  }

  test("DSL should let you create an output maptype column specifying the qualifier field.") {
    val colReq: ColumnRequest = MapFamily("searches")('terms)
  }

  test("DSL should let you specify qualifier regex on maptype columns requests.") {
    val colReq: ColumnFamily = MapFamily("search", qualifierMatches=""".*\.com""")

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.options.filter.get.isInstanceOf[RegexQualifierColumnFilter])
  }

  test("DSL should let you specify versions on maptype column requests without qualifier regex.") {
    val colReq: ColumnFamily = MapFamily("search", versions=2)

    assert(colReq.options.maxVersions == 2)
  }

  test("DSL should let you specify versions on a grouptype column.") {
    val colReq: QualifiedColumn = Column("info:word", versions=3)

    assert(colReq.options.maxVersions == 3)
  }

  test("DSL should have default versions of 1 for maptype and grouptype column requests.") {
    val colReq1: QualifiedColumn = Column("info:word")
    val colReq2: ColumnFamily = MapFamily("searches")

    assert(colReq1.options.maxVersions == 1)
    assert(colReq2.options.maxVersions == 1)
  }

  test("DSL should let you create inputs and outputs with no mappings.") {
    val input: KijiSource = KijiInput(tableURI)()
    val output: KijiSource = KijiOutput(tableURI)()

    assert(input.columns.isEmpty)
    assert(output.columns.isEmpty)
  }

  test("DSL should let you create KijiSources as inputs with default options.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          TimeRange.All,
          None,
          1000,
          Map("word" -> Column("info:word").ignoreMissing))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you specify timerange for KijiInput.") {
    val input = KijiInput(tableURI, timeRange=TimeRange.Between(0L,40L))("info:word" -> 'word)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          TimeRange.Between(0L, 40L),
          None,
          1000,
          Map("word" -> Column("info:word").ignoreMissing))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you create KijiSources with multiple columns.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word, "info:title" -> 'title)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          TimeRange.All,
          None,
          1000,
          Map(
              "word" -> Column("info:word").ignoreMissing,
              "title" -> Column("info:title").ignoreMissing))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you specify options for a column.") {
    val input: KijiSource =
        KijiInput(tableURI)(Map(Column("info:word") -> 'word))
    val input2: KijiSource =
        KijiInput(tableURI)(Map(Column("info:word", versions = 1) -> 'word))
    val input3: KijiSource =
        KijiInput(tableURI)(Map(MapFamily("searches", versions=1, qualifierMatches=".*") -> 'word))
  }

  test("A qualified Column can specify a replacement that is a single value.") {
    val col = Column("family:qualifier").replaceMissingWith("replacement")
    assert(col.isInstanceOf[QualifiedColumn])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumn]
    val replacementOption: Option[KijiSlice[_]] = qualifiedColumn.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.cells.size)
    assert("replacement" === replacement.getFirstValue())
  }

  test("A ColumnFamily can specify a replacement that is a single value.") {
    val col = MapFamily("family")('qualifier).replaceMissingWith("qualifier", "replacement")
    assert(col.isInstanceOf[ColumnFamily])

    val columnFamily = col.asInstanceOf[ColumnFamily]
    val replacementOption: Option[KijiSlice[_]] = columnFamily.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.cells.size)
    assert("replacement" === replacement.getFirstValue())
  }

  test("A qualified Column can specify a replacement that is a single value with a timestamp.") {
    val col = Column("family:qualifier").replaceMissingWithVersioned(10L, "replacement")
    assert(col.isInstanceOf[QualifiedColumn])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumn]
    val replacementOption: Option[KijiSlice[_]] = qualifiedColumn.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.cells.size)
    assert("replacement" === replacement.getFirstValue())
    assert(10L === replacement.getFirst().version)
  }

  test("A ColumnFamily can specify a replacement that is a single value with a timestamp.") {
    val col = MapFamily("family")('qualifier).replaceMissingWithVersioned(
        "qualifier",
        10L,
        "replacement")
    assert(col.isInstanceOf[ColumnFamily])

    val columnFamily = col.asInstanceOf[ColumnFamily]
    val replacementOption: Option[KijiSlice[_]] = columnFamily.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.cells.size)
    assert("replacement" === replacement.getFirstValue())
    assert(10L === replacement.getFirst().version)
  }

  test("A qualified Column can specify a replacement that is multiple values.") {
    val col = Column("family:qualifier").replaceMissingWith(List("replacement1", "replacement2"))
    assert(col.isInstanceOf[QualifiedColumn])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumn]
    val replacementOption: Option[KijiSlice[_]] = qualifiedColumn.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.cells.map { _.datum }

    assert(2 === replacementData.size)
    assert(replacementData.contains("replacement1"))
    assert(replacementData.contains("replacement2"))
  }

  test("A ColumnFamily can specify a replacement that is multiple values.") {
    val col = MapFamily("family")('qualifier)
        .replaceMissingWith(List(("qualifier1", "replacement1"), ("qualifier2", "replacement2")))
    assert(col.isInstanceOf[ColumnFamily])

    val columnFamily = col.asInstanceOf[ColumnFamily]
    val replacementOption: Option[KijiSlice[_]] = columnFamily.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.cells.map { c: Cell[_] => (c.qualifier, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains(("qualifier1", "replacement1")))
    assert(replacementData.contains(("qualifier2", "replacement2")))
  }

  test("A qualified Column can specify a replacement that is multiple values with timestamps.") {
    val col = Column("family:qualifier")
        .replaceMissingWithVersioned(List((10L, "replacement1"), (20L, "replacement2")))
    assert(col.isInstanceOf[QualifiedColumn])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumn]
    val replacementOption: Option[KijiSlice[_]] = qualifiedColumn.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.cells.map { c: Cell[_] => (c.version, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains((10L, "replacement1")))
    assert(replacementData.contains((20L, "replacement2")))
  }

  test("A ColumnFamily can specify a replacement that is multiple values with timestamps.") {
    val col = MapFamily("family")('qualifier)
        .replaceMissingWithVersioned(List(
            ("qualifier1", 10L, "replacement1"),
            ("qualifier2", 20L, "replacement2")))
    assert(col.isInstanceOf[ColumnFamily])

    val columnFamily = col.asInstanceOf[ColumnFamily]
    val replacementOption: Option[KijiSlice[_]] = columnFamily.options.replacementSlice
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.cells.map { c: Cell[_] =>
      (c.qualifier, c.version, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains(("qualifier1", 10L, "replacement1")))
    assert(replacementData.contains(("qualifier2", 20L, "replacement2")))
  }

  test("DSL should let you specify different options for different columns.") {
    val input: KijiSource = KijiInput(tableURI)(
      Map(
        Column("info:word", versions=1) -> 'word,
        Column("info:title", versions=2) -> 'title))
  }

  test("DSL should let you create KijiSource with the default timestamp field") {
    val output: KijiSource = KijiOutput(tableURI)('words -> "info:words")
    val expectedScheme: KijiScheme = {
      new KijiScheme(TimeRange.All, None, 1000, Map("words" -> Column("info:words")))
    }

    assert(expectedScheme == output.hdfsScheme)
  }

  test("DSL should let you create KijiSource with a timestamp field") {
    val output: KijiSource = KijiOutput(tableURI, 'time)('words -> "info:words")
    val expectedScheme: KijiScheme = {
      new KijiScheme(TimeRange.All, Some('time), 1000, Map("words" -> Column("info:words")))
    }

    assert(expectedScheme == output.hdfsScheme)
  }
}
