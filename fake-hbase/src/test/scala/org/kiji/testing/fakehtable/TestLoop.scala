/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.testing.fakehtable

import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory

/** Tests for the breakable Loop construct. */
class TestLoop {
  import TestLoop.Log

  /** Test basic looping. */
  @Test
  def testLoop(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0
    var iloop1 = 0
    loop1 {
      if (iloop1 >= 2) loop1.break()
      iloop1 += 1

      loop1Counter += 1

      var iloop2 = 0
      loop2 {
        if (iloop2 >= 3) loop2.break()
        iloop2 += 1

        loop2Counter += 1
      }
    }
    Assert.assertEquals(2, loop1Counter)
    Assert.assertEquals(6, loop2Counter)
  }

  /** Test loops. */
  @Test
  def testNestedBreak(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0
    loop1 {
      loop1Counter += 1
      loop2 {
        loop2Counter += 1
        loop1.break()
      }
    }
    Assert.assertEquals(1, loop1Counter)
    Assert.assertEquals(1, loop2Counter)
  }

  /** Test loops. */
  @Test
  def testContinue(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0

    var iloop1 = 0
    loop1 {
      if (iloop1 >= 2) loop1.break()
      iloop1 += 1

      loop1Counter += 1

      var iloop2 = 0
      loop2 {
        if (iloop2 >= 1) loop2.break()
        iloop2 += 1

        loop2Counter += 1
        loop1.continue()
        Assert.fail("Should not happen!")
      }

    }
    Assert.assertEquals(2, loop1Counter)
    Assert.assertEquals(2, loop2Counter)
  }
}

object TestLoop {
  private final val Log = LoggerFactory.getLogger(classOf[TestLoop])
}