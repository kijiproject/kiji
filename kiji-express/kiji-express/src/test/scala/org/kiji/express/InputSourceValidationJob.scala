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

package org.kiji.express

import scala.collection.mutable

import cascading.tuple.Fields
import cascading.pipe.Each
import cascading.pipe.Pipe
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.NullSource
import com.twitter.scalding.SideEffectMapFunction
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter

import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.TestPipeConversions

/**
 * A job for testing scalding sources as input. Requires that a source and the resulting tuples it
 * should produce be provided by the user. This job does not write data produced by the input source
 * but instead validates that the tuples produced are correct within a map function.
 *
 * @param inputSource to test.
 * @param expectedTuples tuples that the input source should produce.
 * @param expectedFields that the input source should produce.
 * @param converter for validating that the specified fields arity matches the resulting tuple
 *     arity. This converter should be provided implicitly.
 * @param setter is ignored for this job since no tuples are written out.
 * @tparam A is the tuple type produced by the input source.
 */
class InputSourceValidationJob[A](
    @transient inputSource: Source,
    @transient expectedTuples: Set[A],
    expectedFields: Fields
)(implicit
    converter: TupleConverter[A],
    setter: TupleSetter[Unit]
) extends Job(Args(Nil)) with TestPipeConversions {
  converter.assertArityMatches(expectedFields)

  val _expected: KijiLocker[Set[A]] = KijiLocker(expectedTuples)
  lazy val expectedTuplesDeserialized: Set[A] = _expected.get

  def inputPipe: Pipe = inputSource.read
  def recordPipe: Each = new Each(
      inputPipe,
      Fields.ALL,
      new SideEffectMapFunction(
          bf = { mutable.ArrayBuffer[A]() },
          fn = { (buffer: mutable.ArrayBuffer[A], tuple: A) => buffer.append(tuple) },
          ef = InputSourceValidationJob.assertOutput(expectedTuplesDeserialized),
          fields = Fields.NONE,
          converter,
          setter
      )
  )
  NullSource.writeFrom(recordPipe)
}

object InputSourceValidationJob {
  def assertOutput[T](expected: Set[T])(actual: Seq[T]) {
    val actualSet = actual.toSet
    assert(
        actualSet == expected,
        "actual: %s\nexpected: %s\noutput missing: %s\nunexpected: %s".format(
            actualSet,
            expected,
            expected -- actualSet,
            actualSet -- expected
        )
    )
  }
}

