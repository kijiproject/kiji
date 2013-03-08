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

import scala.collection.JavaConversions.mapAsJavaMap

import org.scalatest.FunSuite

import org.kiji.chopsticks.DSL._
import org.kiji.lang.KijiScheme

class DSLSuite extends FunSuite {
  val tableURI = "kiji://.env/default/table"

  test("DSL should let you create inputs and outputs with no mappings.") {
    val input: KijiSource = KijiInput(tableURI)()
    val output: KijiSource = KijiOutput(tableURI)()

    assert(input.columns.isEmpty())
    assert(output.columns.isEmpty())
  }

  test("DSL should let you create KijiSources as inputs with default options.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word)
    val expectedScheme: KijiScheme = new KijiScheme(Map("word" -> Column("info:word")))

    assert(expectedScheme == input.kijiScheme)
  }

  test("DSL should let you specify timerange for KijiInput.") {
    pending
    // TODO(CHOP-36): After CHOP-36, the test should look something like this:
    // val input = KijiInput(tableURI, timeRange=(0L,40L))("info:word" -> 'word)
  }

  test("DSL should let you create KijiSources with multiple columns.") {
    val input: KijiSource = KijiInput(tableURI)("info:word" -> 'word, "info:title" -> 'title)
    val expectedScheme: KijiScheme = new KijiScheme(
      Map("word" -> Column("info:word"), "title" -> Column("info:title")))

    assert(expectedScheme == input.kijiScheme)
  }

  test("DSL should let you specify inputOptions for a column.") {
    val input: KijiSource =
      KijiInput(tableURI, Map(Column("info:word", InputOptions()) -> 'word))
    val input2: KijiSource =
      KijiInput(tableURI, Map(Column("info:word", InputOptions(maxVersions=1)) -> 'word))
    val input3: KijiSource = KijiInput(tableURI,
        Map(Column("info:word", InputOptions(maxVersions=1, filter=null)) -> 'word))
  }

  test("DSL should let you specify different inputOptions for different columns.") {
    val input: KijiSource = KijiInput(tableURI,
      Map(
        Column("info:word", InputOptions(maxVersions=1)) -> 'word,
        Column("info:title", InputOptions(maxVersions=2)) -> 'title))
  }

  test("DSL should let you create KijiSources as outputs.") {
    val output: KijiSource = KijiOutput(tableURI)('words -> "info:words")
    val expectedScheme: KijiScheme = new KijiScheme(Map("words" -> Column("info:words")))

    assert(expectedScheme == output.kijiScheme)
  }
}
