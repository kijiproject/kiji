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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.framework.ExpressConversions
import org.kiji.express.flow.util.PipeConversions

import com.twitter.scalding.Hdfs
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Mode
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter

import cascading.flow.FlowDef
import cascading.pipe.Pipe

/**
 * Object containing various implicit conversions required to create Scalding flows in the REPL
 * and in other Pipe extensions.
 * Most of these conversions come from Scalding's Job class.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object Implicits
    extends ExpressConversions
    with PipeConversions {
  /** Implicit flowDef for this KijiExpress shell session. */
  implicit var flowDef: FlowDef = getEmptyFlowDef

  /** Mode that the current REPL job is setup to use. */
  implicit var mode: Mode = Hdfs(strict = true, conf = HBaseConfiguration.create())

  /**
   * Sets the flow definition in implicit scope to an empty flow definition.
   */
  def resetFlowDef() {
    flowDef = getEmptyFlowDef
  }

  /**
   * Gets a new, empty, flow definition.
   *
   * @return a new, empty flow definition.
   */
  private[express] def getEmptyFlowDef: FlowDef = {
    val fd = new FlowDef
    fd.setName("ExpressShell")
    fd
  }

  /**
   * Converts an iterable into a Source with index (int-based) fields.
   *
   * @param iterable to convert into a Source.
   * @param setter implicitly retrieved and used to convert the specified iterable into a Source.
   * @param converter implicitly retrieved and used to convert the specified iterable into a Source.
   * @return a Source backed by the specified iterable.
   */
  implicit def iterableToSource[T](
      iterable: Iterable[T]
  )(implicit
      setter: TupleSetter[T],
      converter: TupleConverter[T]
  ): Source = {
    IterableSource[T](iterable)(setter, converter)
  }

  /**
   * Converts an iterable into a Pipe with index (int-based) fields.
   *
   * @param iterable to convert into a Pipe.
   * @param setter implicitly retrieved and used to convert the specified iterable into a Pipe.
   * @param converter implicitly retrieved and used to convert the specified iterable into a Pipe.
   * @return a Pipe backed by the specified iterable.
   */
  implicit def iterableToPipe[T](
      iterable: Iterable[T]
  )(implicit
      setter: TupleSetter[T],
      converter: TupleConverter[T]
  ): Pipe = {
    iterableToSource(iterable)(setter, converter).read(flowDef, mode)
  }

  /**
   * Converts an iterable into a RichPipe with index (int-based) fields.
   *
   * @param iterable to convert into a RichPipe.
   * @param setter implicitly retrieved and used to convert the specified iterable into a RichPipe.
   * @param converter implicitly retrieved and used to convert the specified iterable into a
   *     RichPipe.
   * @return a RichPipe backed by the specified iterable.
   */
  implicit def iterableToRichPipe[T](
      iterable: Iterable[T]
  )(implicit setter: TupleSetter[T],
      converter: TupleConverter[T]
  ): RichPipe = {
    RichPipe(iterableToPipe(iterable)(setter, converter))
  }

  /**
   * Converts a Source to a RichPipe. This method permits implicit conversions from Source to
   * RichPipe.
   *
   * @param source to convert to a RichPipe.
   * @return a RichPipe wrapping the result of reading the specified Source.
   */
  implicit def sourceToRichPipe(source: Source): RichPipe = RichPipe(source.read(flowDef, mode))

  /**
   * Converts a Source to a Pipe. This method permits implicit conversions from Source to Pipe.
   *
   * @param source to convert to a Pipe.
   * @return a Pipe that is the result of reading the specified Source.
   */
  implicit def sourceToPipe(source: Source): Pipe = source.read(flowDef, mode)
}
