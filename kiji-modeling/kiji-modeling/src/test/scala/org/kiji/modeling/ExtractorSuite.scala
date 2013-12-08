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

package org.kiji.modeling

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

class TestExtractor extends Extractor {
  override val extractFn = extract('textLine -> 'count) { line: String =>
    line
        .split("""\s+""")
        .size
  }
}

class TestMalformedExtractor extends Extractor {
  override val extractFn = extract(('field1, 'field2) -> 'count) { line: String =>
    line
  }
}

class TestMultipleArgumentExtractor extends Extractor {
  override val extractFn = extract(('line1, 'line2) -> 'count) { lines: (String, String) =>
    val (line1, line2) = lines

    (line1 + line2)
        .split("""\s+""")
        .size
  }
}

class TestMultipleReturnValuesExtractor extends Extractor {
  override val extractFn = extract('line -> ('count1, 'count)) { line: String =>
    val count = line
        .split("""\s+""")
        .size

    (count, count * 2)
  }
}

@RunWith(classOf[JUnitRunner])
class ExtractorSuite extends FunSuite {
  test("An extractor can be constructed via reflection") {
    val actual: Extractor = classOf[TestExtractor].newInstance
    val expected: Extractor = new TestExtractor

    val actualFields = actual
        .extractFn
        .fields
    val expectedFields = expected
        .extractFn
        .fields
    assert(expectedFields === actualFields)

    val actualFn: (String => Int) = actual
        .extractFn
        .fn
        .asInstanceOf[(String => Int)]
    val expectedFn: (String => Int) = expected
        .extractFn
        .fn
        .asInstanceOf[(String => Int)]
    assert(expectedFn("foo bar") === actualFn("foo bar"))
  }

  test("An extractor defined with invalid field selection will fail on construction") {
    intercept[AssertionError] {
      new TestMalformedExtractor
    }
  }

  test("An extractor with multiple arguments") {
    val extractor: Extractor = new TestMultipleArgumentExtractor

    val fn: ((String, String)) => Int = extractor
        .extractFn
        .fn
        .asInstanceOf[((String, String)) => Int]
    assert(2 === fn(("hello ", "world")))
  }

  test("An extractor with multiple returned values") {
    val extractor: Extractor = new TestMultipleReturnValuesExtractor

    val fn: String => (Int, Int) = extractor
        .extractFn
        .fn
        .asInstanceOf[String => (Int, Int)]
    assert((2, 4) === fn("hello world"))
  }
}
