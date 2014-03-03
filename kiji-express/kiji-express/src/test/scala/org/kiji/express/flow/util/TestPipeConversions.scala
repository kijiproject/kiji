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

package org.kiji.express.flow.util

import scala.collection.mutable

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.pipe.Each
import cascading.tuple.Fields
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.RichPipe
import com.twitter.scalding.SideEffectMapFunction
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.KijiSuite

@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
private[express] trait TestPipeConversions {
  implicit def pipe2TestPipe(pipe: Pipe): TestPipe = new TestPipe(pipe)
}

object TestPipe {
  val logger: Logger = LoggerFactory.getLogger(classOf[KijiSuite])
}

class TestPipe(val pipe: Pipe)
  extends FieldConversions
  with TestPipeConversions
  with Serializable
{
  implicit def pipeToRichPipe(pipe : Pipe): RichPipe = new RichPipe(pipe)

  def assertOutputValues[A](
      fields: Fields,
      expected: Set[A]
  )(implicit
      conv: TupleConverter[A],
      set: TupleSetter[Unit],
      flowDef: FlowDef,
      mode: Mode
  ) {
    conv.assertArityMatches(fields)

    def bf: mutable.Set[A] = mutable.Set[A]()

    def ef(mySet: mutable.Set[A]): Unit = {
      val output: Set[A] = mySet.toSet

      require(output.size == expected.size)
      output.foreach { tuple =>
        require(expected.contains(tuple), "%s is missing %s".format(expected, tuple))
      }

      if (expected == output) {
        TestPipe.logger.debug("Confirmed values on pipe!")
      } else {
        TestPipe.logger.debug("Mismatch in expected value for pipe and output value")

        val inExpectedNotFound: Set[A] = expected -- output
        TestPipe.logger
          .debug("Values in expected output that were not found: " + inExpectedNotFound)

        val foundButNotExpected: Set[A] = output -- expected
        TestPipe.logger.debug("Values found but not expected: " + foundButNotExpected)

        throw new Exception("MISMATCH!")
      }
    }

    def fn(mySet: mutable.Set[A], tup: A): Unit = { mySet += tup }

    val newPipe = new Each(
        pipe,
        fields,
        new SideEffectMapFunction(bf, fn, ef, Fields.NONE, conv, set)
    )
    NullSource.writeFrom(newPipe)(flowDef, mode)
  }
}
