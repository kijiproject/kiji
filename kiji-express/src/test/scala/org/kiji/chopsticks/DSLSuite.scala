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

import java.lang.IllegalArgumentException

import scala.collection.JavaConversions.mapAsJavaMap

import org.scalatest.FunSuite

import org.kiji.chopsticks.DSL._
import org.kiji.schema.filter.RegexQualifierColumnFilter

class DSLSuite extends FunSuite {
  val tableURI = "kiji://.env/default/table"

  test("DSL should not let you create a grouptype column without a qualifier.") {
    intercept[IllegalArgumentException] {
      val colReq: ColumnRequest = Column("search")
    }
  }

  test("DSL should not let you create a maptype column with a qualifier.") {
    intercept[IllegalArgumentException] {
      val colReq: ColumnRequest = MapColumn("info:word")
    }
  }

  test("DSL should let you specify qualifier regex on maptype columns.") {
    val colReq: ColumnRequest = MapColumn("search", qualifierMatches=""".*\.com""")

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.inputOptions.filter.isInstanceOf[RegexQualifierColumnFilter])
  }

  test("DSL should let you specify versions on maptype column without qualifier regex.") {
    val colReq: ColumnRequest = MapColumn("search", versions=2)

    assert(colReq.inputOptions.maxVersions == 2)
  }

  test("DSL should let you specify versions on a grouptype column.") {
    val colReq: ColumnRequest = Column("info:word", versions=3)

    assert(colReq.inputOptions.maxVersions == 3)
  }

  test("DSL should have default versions of 1 for maptype and grouptype columns.") {
    val colReq1: ColumnRequest = Column("info:word")
    val colReq2: ColumnRequest = MapColumn("searches")

    assert(colReq1.inputOptions.maxVersions == 1)
    assert(colReq2.inputOptions.maxVersions == 1)
  }

  test("DSL should let you create inputs and outputs with no mappings.") {
    val input: KijiSource = KijiInput(tableURI)()
    val output: KijiSource = KijiOutput(tableURI)()

    assert(input.columns.isEmpty())
    assert(output.columns.isEmpty())
  }

  test("DSL should let you create KijiSources as inputs with default options.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word)
    val expectedScheme: KijiScheme = {
      new KijiScheme(TimeRange.All, Map("word" -> Column("info:word")))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you specify timerange for KijiInput.") {
    val input = KijiInput(tableURI, timeRange=TimeRange.Between(0L,40L))("info:word" -> 'word)
    val expectedScheme: KijiScheme = {
      new KijiScheme(TimeRange.Between(0L, 40L), Map("word" -> Column("info:word")))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you create KijiSources with multiple columns.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word, "info:title" -> 'title)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          TimeRange.All,
          Map("word" -> Column("info:word"), "title" -> Column("info:title")))
    }

    assert(expectedScheme == input.hdfsScheme)
  }

  test("DSL should let you specify inputOptions for a column.") {
    val input: KijiSource =
      KijiInput(tableURI, Map(Column("info:word") -> 'word))
    val input2: KijiSource =
      KijiInput(tableURI, Map(Column("info:word", versions = 1) -> 'word))
    val input3: KijiSource = KijiInput(tableURI,
        Map(MapColumn("searches", versions=1, qualifierMatches=".*") -> 'word))
  }

  test("DSL should let you specify different inputOptions for different columns.") {
    val input: KijiSource = KijiInput(tableURI,
      Map(
        Column("info:word", versions=1) -> 'word,
        Column("info:title", versions=2) -> 'title))
  }

  test("DSL should let you create KijiSources as outputs.") {
    val output: KijiSource = KijiOutput(tableURI)('words -> "info:words")
    val expectedScheme: KijiScheme = {
      new KijiScheme(TimeRange.All, Map("words" -> Column("info:words")))
    }

    assert(expectedScheme == output.hdfsScheme)
  }
}
