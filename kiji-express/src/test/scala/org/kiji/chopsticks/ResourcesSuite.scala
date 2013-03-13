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

import java.io.Closeable

import org.scalatest.FunSuite

import org.kiji.chopsticks.Resources._

class ResourcesSuite extends FunSuite {
  test("doAnd runs an operation") {
    var isClosed = false
    def resource: Closeable = new Closeable {
      def close() {
        isClosed = true
      }
    }
    def after(r: Closeable) { r.close() }
    doAnd(resource, after) { _ => () }

    assert(isClosed)
  }

  test("doAnd catches one normal exception") {
    var isClosed = false
    def resource: Closeable = new Closeable {
      def close() {
        isClosed = true
      }
    }
    def after(r: Closeable) { r.close() }
    val result = try {
      doAnd(resource, after) { _ => sys.error("test") }
      false
    } catch {
      case err: RuntimeException => true
    }

    assert(isClosed)
    assert(result)
  }

  test("doAnd catches one exception while closing") {
    def resource: Closeable = new Closeable {
      def close() {
        sys.error("test")
      }
    }
    def after(r: Closeable) { r.close() }
    val result = try {
      doAnd(resource, after) { _ => () }
      false
    } catch {
      case err: RuntimeException => true
    }

    assert(result)
  }

  test("doAnd catches two exceptions") {
    def resource: Closeable = new Closeable {
      def close() {
        sys.error("test")
      }
    }
    def after(r: Closeable) { r.close() }
    val result = try {
      doAnd(resource, after) { _ => sys.error("t") }
      false
    } catch {
      case CompoundException(_, Seq(_, _)) => true
    }
  }
}
