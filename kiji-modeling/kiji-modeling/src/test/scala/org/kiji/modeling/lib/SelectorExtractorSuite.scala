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

package org.kiji.modeling.lib

import cascading.tuple.Fields
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.flow.FlowCell

@RunWith(classOf[JUnitRunner])
class SelectorExtractorSuite extends FunSuite {
  val slice1 = Seq(
      new FlowCell("family", "qualifier", 3L, "bar1"),
      new FlowCell("family", "qualifier", 2L, "baz1"),
      new FlowCell("family", "qualifier", 1L, "foo1"))
  val slice2 = Seq(
      new FlowCell("family", "qualifier", 3L, "bar2"),
      new FlowCell("family", "qualifier", 2L, "baz2"),
      new FlowCell("family", "qualifier", 1L, "foo2"))
  val firstValueExtractor = new FirstValueExtractor
  val lastValueExtractor = new LastValueExtractor
  val sliceExtractor = new SliceExtractor

  test("SelectorExtractor should use wildcard input and output fields") {
    // Verify that the extract phase's input and output fields are as expected.
    assert(Fields.ALL === firstValueExtractor.extractFn.fields._1)
    assert(Fields.RESULTS === firstValueExtractor.extractFn.fields._2)

    // Verify that the extract phase's input and output fields are as expected.
    assert(Fields.ALL === lastValueExtractor.extractFn.fields._1)
    assert(Fields.RESULTS === lastValueExtractor.extractFn.fields._2)

    // Verify that the extract phase's input and output fields are as expected.
    assert(Fields.ALL === sliceExtractor.extractFn.fields._1)
    assert(Fields.RESULTS === sliceExtractor.extractFn.fields._2)
  }

  test("FirstValueExtractor should select the first value of a sequence of cells") {
    assert(Tuple1("bar1") === firstValueExtractor.extractFn.fn(Tuple1(slice1)))
  }

  test("FirstValueExtractor should select the first values of a tuple of sequences of cells") {
    assert(("bar1", "bar2") === firstValueExtractor.extractFn.fn((slice1, slice2)))
  }

  test("LastValueExtractor should select the last value of a sequence of cells") {
    assert(Tuple1("foo1") === lastValueExtractor.extractFn.fn(Tuple1(slice1)))
  }

  test("LastValueExtractor should select the last values of a tuple of sequences of cells") {
    assert(("foo1", "foo2") === lastValueExtractor.extractFn.fn((slice1, slice2)))
  }

  test("SliceExtractor should select the values in a sequence of cells") {
    val expected = Tuple1(Seq("bar1", "baz1", "foo1"))
    assert(expected === sliceExtractor.extractFn.fn(Tuple1(slice1)))
  }

  test("SliceExtractor should select the values in a tuple of sequences of cells") {
    val expected = (Seq("bar1", "baz1", "foo1"), Seq("bar2", "baz2", "foo2"))
    assert(expected === sliceExtractor.extractFn.fn((slice1, slice2)))
  }
}
