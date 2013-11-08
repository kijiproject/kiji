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
import org.scalatest.mock.EasyMockSugar

import org.kiji.mapreduce.kvstore.{KeyValueStore => JKeyValueStore}
import org.kiji.mapreduce.kvstore.{KeyValueStoreReader => JKeyValueStoreReader}
import org.kiji.modeling.impl.ForwardingKeyValueStore

/**
 * Tests the basic functionality of [[org.kiji.modeling.KeyValueStore]]. These tests do
 * not include tests that use specific backing KijiSchema key-value stores.
 */
@RunWith(classOf[JUnitRunner])
class KeyValueStoreSuite extends FunSuite with EasyMockSugar {

  /**
   * A key-value store that can be instantiated using a mock KijiSchema key-value store. This
   * implementation is used in the tests below.
   */
  private def dummyKVStore(kvStore: JKeyValueStore[String, Integer]): KeyValueStore[String, Int] = {
    def convertValue(value: Integer): Int = value
    new ForwardingKeyValueStore[String, Int, String, Integer](
        kvStore.open(),
        identity,
        convertValue)
  }

  /**
   * Gets a mock KijiSchema key-value store and key-value store reader for use in tests.
   *
   * @return a pair containing the mocked key-value store and key-value store reader. The mocked
   *     key-value store is already expected to be opened.
   */
  private def getMocks: (JKeyValueStore[java.lang.String, java.lang.Integer],
      JKeyValueStoreReader[java.lang.String, java.lang.Integer]) = {
    // A KijiSchema key-value store and key-value store reader mocked for tests.
    val mockJKeyValueStore = mock[JKeyValueStore[java.lang.String, java.lang.Integer]]
    val mockJKeyValueStoreReader: JKeyValueStoreReader[java.lang.String, java.lang.Integer] =
      mock[JKeyValueStoreReader[java.lang.String, java.lang.Integer]]
    // In all tests, we expect the mock KijiSchema key-value store to be opened.
    expecting {
      mockJKeyValueStore.open().andReturn(mockJKeyValueStoreReader)
    }
    (mockJKeyValueStore, mockJKeyValueStoreReader)
  }

  test("A KijiExpress KeyValueStore can be instantiated from a KijiSchema KeyValueStore.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      dummyKVStore(mockJKeyValueStore)
    }
  }

  test("Closing a KijiExpress KeyValueStore closes the underlying KeyValueStoreReader.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    expecting {
      mockJKeyValueStoreReader.close()
    }
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      kvStore.close()
    }
  }

  test("The value for a key that exists can be retrieved from a KeyValueStore using apply.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    val valueRetrieved: Int = 5684
    expecting {
      mockJKeyValueStoreReader
          .get(keyToLookup.asInstanceOf[java.lang.String])
          .andReturn(valueRetrieved.asInstanceOf[java.lang.Integer])
    }
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val actualValueRetrieved = kvStore(keyToLookup)
      assert(valueRetrieved === actualValueRetrieved,
          "Unexpected value retrieved from KeyValueStore.")
    }
  }

  test("KeyValueStore throws NoSuchElementException when using apply with non-existent key.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    // scalastyle:off null
    expecting {
      mockJKeyValueStoreReader
      .get(keyToLookup.asInstanceOf[java.lang.String])
      .andReturn(null)
    }
    // scalastyle:on null
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val thrown = intercept[NoSuchElementException] {
        kvStore(keyToLookup)
      }
      assert(thrown.getMessage.startsWith("No value exists in this KeyValueStore for key 'key'"))
    }
  }

  test("The optional value for a key can be retrieved from a KeyValueStore using get.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    val valueRetrieved: Int = 5684
    expecting {
      mockJKeyValueStoreReader
      .get(keyToLookup.asInstanceOf[java.lang.String])
      .andReturn(valueRetrieved.asInstanceOf[java.lang.Integer])
    }
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val actualValueRetrieved: Option[Int] = kvStore.get(keyToLookup)
      assert(actualValueRetrieved.isDefined, "Could not retrieve defined value from KeyValueStore")
      assert(valueRetrieved === actualValueRetrieved.get,
        "Unexpected value retrieved from KeyValueStore.")
    }
  }

  test("A KeyValueStore retrieves None for a key that does not exist using get.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    // scalastyle:off null
    expecting {
      mockJKeyValueStoreReader
      .get(keyToLookup.asInstanceOf[java.lang.String])
      .andReturn(null)
    }
    // scalastyle:on null
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val actualValueRetrieved: Option[Int] = kvStore.get(keyToLookup)
      assert(!actualValueRetrieved.isDefined,
        "Some value retrieved from KeyValueStore for key that does not exist.")
    }
  }

  test("A KeyValueStore throws an IllegalArgumentException if a null key is passed to apply.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val thrown = intercept[IllegalArgumentException] {
        // scalastyle:off null
        kvStore(null)
        // scalastyle:on null
      }
      assert(thrown.getMessage.contains(
          "A null key was used to access a value from a KeyValueStore"))
    }
  }

  test("A KeyValueStore throws an IllegalArgumentException if a null key is passed to get.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      val thrown = intercept[IllegalArgumentException] {
        // scalastyle:off null
        kvStore.get(null)
        // scalastyle:on null
      }
      assert(thrown.getMessage.contains(
          "A null key was used to access a value from a KeyValueStore"))
    }
  }

  test("A KeyValueStore can be queried accurately to determine if a key exists.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    expecting {
      mockJKeyValueStoreReader
          .containsKey(keyToLookup.asInstanceOf[java.lang.String])
          .andReturn(true)
    }
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      assert(kvStore.containsKey(keyToLookup),
          "KeyValueStore reports that it does not contain a key which it does.")
    }
  }

  test("A KeyValueStore can be queried accurately to determine if a key does not exist.") {
    val (mockJKeyValueStore, mockJKeyValueStoreReader) = getMocks
    val keyToLookup: String = "key"
    expecting {
      mockJKeyValueStoreReader
          .containsKey(keyToLookup.asInstanceOf[java.lang.String])
          .andReturn(false)
    }
    whenExecuting(mockJKeyValueStore, mockJKeyValueStoreReader) {
      val kvStore = dummyKVStore(mockJKeyValueStore)
      assert(!kvStore.containsKey(keyToLookup),
          "KeyValueStore reports that it contains a key which it does not.")
    }
  }
}
