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

package org.kiji.express.util

import org.scalatest.FunSuite

class MemoizeSuite extends FunSuite {
  test("Test for memoization") {
    // outer class without equals defined
    class Outer(val data: Int)

    def strSqLen(s: String): Outer = new Outer(s.length * s.length)
    val strSqLenMemoized = Memoize(strSqLen)
    val a = strSqLenMemoized("hello Memo")
    val b = strSqLen("hello Memo")
    assert(a.data == b.data)
    // should go to cache for result
    val c = strSqLenMemoized("hello Memo")
    assert(a == c)
  }
}
