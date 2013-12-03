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

package org.kiji.express.repl

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.IterableSource
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.AvroTupleConversions
import org.kiji.express.flow.util.PipeConversions

/**
 * Object containing various implicit conversions required to create Scalding flows in the REPL.
 * Most of these conversions come from Scalding's Job class.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object Implicits
    extends PipeConversions
    with TupleConversions
    with FieldConversions
    with AvroTupleConversions {
  /** Implicit flowDef for this KijiExpress shell session. */
  implicit var flowDef: FlowDef = getEmptyFlowDef

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
   * Converts a Cascading Pipe to a Scalding RichPipe. This method permits implicit conversions from
   * Pipe to RichPipe.
   *
   * @param pipe to convert to a RichPipe.
   * @return a RichPipe wrapping the specified Pipe.
   */
  implicit def pipeToRichPipe(pipe: Pipe): RichPipe = new RichPipe(pipe)

  /**
   * Converts a Scalding RichPipe to a Cascading Pipe. This method permits implicit conversions from
   * RichPipe to Pipe.
   *
   * @param richPipe to convert to a Pipe.
   * @return the Pipe wrapped by the specified RichPipe.
   */
  implicit def richPipeToPipe(richPipe: RichPipe): Pipe = richPipe.pipe

  /**
   * Converts a Source to a RichPipe. This method permits implicit conversions from Source to
   * RichPipe.
   *
   * @param source to convert to a RichPipe.
   * @return a RichPipe wrapping the result of reading the specified Source.
   */
  implicit def sourceToRichPipe(source: Source): RichPipe = RichPipe(source.read)

  /**
   * Converts a Source to a Pipe. This method permits implicit conversions from Source to Pipe.
   *
   * @param source to convert to a Pipe.
   * @return a Pipe that is the result of reading the specified Source.
   */
  implicit def sourceToPipe(source: Source): Pipe = source.read

  /**
   * Converts a Pipe to a KijiPipeTool. This method permits implicit conversions from Pipe to
   * KijiPipeTool.
   *
   * @param pipe to convert to a KijiPipeTool.
   * @return a KijiPipeTool created from the specified Pipe.
   */
  implicit def pipeToKijiPipeTool(pipe: Pipe): KijiPipeTool = new KijiPipeTool(pipe)

  /**
   * Converts a KijiPipeTool to a Pipe. This method permits implicit conversions from
   * KijiPipeTool to Pipe.
   *
   * @param kijiPipeTool to convert to a Pipe.
   * @return the Pipe wrapped by the specified KijiPipeTool.
   */
  implicit def kijiPipeToolToPipe(kijiPipeTool: KijiPipeTool): Pipe = kijiPipeTool.pipe

  /**
   * Converts an iterable into a Source with index (int-based) fields.
   *
   * @param iterable to convert into a Source.
   * @param setter implicitly retrieved and used to convert the specified iterable into a Source.
   * @param converter implicitly retrieved and used to convert the specified iterable into a Source.
   * @return a Source backed by the specified iterable.
   */
  implicit def iterableToSource[T](
      iterable: Iterable[T])
      (implicit setter: TupleSetter[T],
          converter: TupleConverter[T]): Source = {
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
      iterable: Iterable[T])
      (implicit setter: TupleSetter[T],
          converter: TupleConverter[T]): Pipe = {
    iterableToSource(iterable)(setter, converter).read
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
      iterable: Iterable[T])
      (implicit setter: TupleSetter[T],
          converter: TupleConverter[T]): RichPipe = {
    RichPipe(iterableToPipe(iterable)(setter, converter))
  }
}
