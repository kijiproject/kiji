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

import org.scalatest.FunSuite

import org.kiji.schema.KConstants

class TimeRangeSuite extends FunSuite {
  test("TimeRange fails fast when invalid time range arguments are specified.") {
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      TimeRange.Between(10L, 1L)
    }

    val expectedMessage = "requirement failed: Invalid time range specified: (%d, %d)"
        .format(10L, 1L)
    assert(thrown.getMessage == expectedMessage)
  }

  test("All constructs a TimeRange correctly") {
    val range: TimeRange = TimeRange.All

    assert(range.begin == KConstants.BEGINNING_OF_TIME)
    assert(range.end == KConstants.END_OF_TIME)
  }

  test("At constructs a TimeRange correctly") {
    val range: TimeRange = TimeRange.At(42L)

    assert(range.begin == 42L)
    assert(range.end == 42L)
  }

  test("After constructs a TimeRange correctly") {
    val range: TimeRange = TimeRange.After(42L)

    assert(range.begin == 42L)
    assert(range.end == KConstants.END_OF_TIME)
  }

  test("Before constructs a TimeRange correctly") {
    val range: TimeRange = TimeRange.Before(42L)

    assert(range.begin == KConstants.BEGINNING_OF_TIME)
    assert(range.end == 42L)
  }

  test("Between constructs a TimeRange correctly") {
    val range: TimeRange = TimeRange.Between(10L, 42L)

    assert(range.begin == 10L)
    assert(range.end == 42L)
  }
}
