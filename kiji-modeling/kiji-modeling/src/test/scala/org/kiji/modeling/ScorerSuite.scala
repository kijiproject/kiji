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

class TestScorer extends Scorer {
  override val scoreFn = score('textLine) { line: String =>
    line
        .split("""\s+""")
        .size
  }
}

class TestMalformedScorer extends Scorer {
  override val scoreFn = score(('field1, 'field2)) { line: String =>
    line
  }
}

class TestMultipleArgumentScorer extends Scorer {
  override val scoreFn = score(('line1, 'line2)) { lines: (String, String) =>
    val (line1, line2) = lines

    (line1 + line2)
        .split("""\s+""")
        .size
  }
}

@RunWith(classOf[JUnitRunner])
class ScorerSuite extends FunSuite {
  test("An scorer can be constructed via reflection") {
    val actual: Scorer = classOf[TestScorer].newInstance
    val expected: Scorer = new TestScorer

    val actualFields = actual
        .scoreFn
        .fields
    val expectedFields = expected
        .scoreFn
        .fields
    assert(expectedFields === actualFields)

    val actualFn: (String => Int) = actual
        .scoreFn
        .fn
        .asInstanceOf[(String => Int)]
    val expectedFn: (String => Int) = expected
        .scoreFn
        .fn
        .asInstanceOf[(String => Int)]
    assert(expectedFn("foo bar") === actualFn("foo bar"))
  }

  test("An scorer defined with invalid field selection will fail on construction") {
    intercept[AssertionError] {
      new TestMalformedScorer
    }
  }

  test("An scorer with multiple arguments") {
    val scorer: Scorer = new TestMultipleArgumentScorer

    val fn: ((String, String)) => Int = scorer
        .scoreFn
        .fn
        .asInstanceOf[((String, String)) => Int]
    assert(2 === fn(("hello ", "world")))
  }
}
