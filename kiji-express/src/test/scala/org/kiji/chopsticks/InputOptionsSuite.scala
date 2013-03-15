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

import org.kiji.chopsticks.Column.InputOptions
import org.kiji.schema.filter.RegexQualifierColumnFilter

class InputOptionsSuite extends FunSuite {
  // TODO(CHOP-37): Test with different filters once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filter = new RegexQualifierColumnFilter(".*")
  val filter = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2
  val opts = new InputOptions(maxVersions, filter)

  test("maxVersions is the same as constructed with.") {
    assert(maxVersions == opts.maxVersions)
  }

  test("inputOptions is the same as constructed with.") {
    assert(filter == opts.filter)
  }

  test("InputOptions with the same maxVersions & filter are equal and hash to the same value.") {
    val opts2 = new InputOptions(maxVersions, filter)

    assert(opts2 == opts)
    assert(opts2.hashCode == opts.hashCode)
  }
}
