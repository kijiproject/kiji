/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.commons.scala

import com.google.common.base.Optional
import org.junit.Assert
import org.junit.Test

class TestJavaConverters {

  private def testScalaToJava[T](
      option: Option[T]
  ): Unit = {
    import org.kiji.commons.scala.JavaConverters.OptionAsOptional
    val optional: Optional[T] = option.asJava
    option match {
      case Some(x) => Assert.assertEquals(x, optional.get)
      case None => Assert.assertFalse(optional.isPresent)
    }
  }

  private def testJavaToScala[T](
      optional: Optional[T]
  ): Unit = {
    import org.kiji.commons.scala.JavaConverters.OptionalAsOption
    val option: Option[T] = optional.asScala
    option match {
      case Some(x) => Assert.assertEquals(optional.get, x)
      case None => Assert.assertFalse(optional.isPresent)
    }
  }

  @Test
  def converts(): Unit = {
    testScalaToJava(Some("abc"))
    testScalaToJava(Some(1))
    testScalaToJava(None)

    testJavaToScala(Optional.of("abc"))
    testJavaToScala(Optional.of(1))
    testJavaToScala(Optional.absent())
  }

  @Test
  def convertsSomeOfNull(): Unit = {
    import JavaConverters.OptionAsOptional
    val option: Option[String] = Some(null)
    val optional: Optional[String] = option.asJava
    Assert.assertFalse(optional.isPresent)
  }
}
