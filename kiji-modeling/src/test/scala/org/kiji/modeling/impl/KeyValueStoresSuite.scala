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

package org.kiji.modeling.impl

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.modeling.KeyValueStore

class TestKeyValueStores extends KeyValueStores

@RunWith(classOf[JUnitRunner])
class KeyValueStoresSuite extends FunSuite {
  test("A concrete instance of KeyValueStores can have its key-value stores property initialized") {
    val kvstoresInstance: TestKeyValueStores = new TestKeyValueStores
    val expected: Map[String, KeyValueStore[_, _]] = Map()

    kvstoresInstance.keyValueStores = expected
    val actual = kvstoresInstance.keyValueStores

    assert(expected === actual)
  }

  test("An uninitialized instance of KeyValueStores will throw an IllegalStateException") {
    val kvstoresInstance: TestKeyValueStores = new TestKeyValueStores
    val thrown = intercept[IllegalStateException] {
      kvstoresInstance.keyValueStores
    }
    assert("This model phase has not been initialized properly. "
        + "Its key-value stores haven't been loaded yet." === thrown.getMessage)
  }
}
