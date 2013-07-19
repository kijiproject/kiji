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

package org.kiji.express.modeling

import cascading.pipe.Pipe
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TupleConversions

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.PipeConversions
import org.kiji.express.repl.Implicits

/**
 * Represents the Prepare phase of a model lifecycle. Users should extend this trait when defining a
 * Prepare phase. The Prepare phase of a model lifecycle is responsible for performing computations
 * required by one or more other lifecycle phases.
 *
 * To define a custom prepare phase, the prepare method of the must be overridden. This method takes
 * as a parameter an already configured pipe and is expected to return a pipe as well. Both input
 * and output configurations are stored in a [[org.kiji.express.modeling.ModelEnvironment]]. For
 * example:
 * {{{
 *   class MyPreparer extends Preparer {
 *     override def prepare(rows: RichPipe): RichPipe = {
 *       rows
 *           .map('inputField -> 'intermediateField) { inputColumn: KijiSlice[String] =>
 *             inputColumn.getFirstValue
 *           }
 *           .groupBy('intermediateField) { _.count('count) }
 *     }
 *   }
 * }}}
 *
 * This trait also provides access to outside data sources required for the Prepare phase through
 * the `kvstores` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
trait Preparer
    extends KeyValueStores
    with PipeConversions
    with FieldConversions
    with TupleConversions {
  /**
   * Used to define the computation required for the Prepare phase of the model lifecycle.
   *
   * @param input pipe to the Prepare phase.
   * @return the computation to perform during the Prepare phase of the model lifecycle.
   */
  def prepare(input: RichPipe): RichPipe

  /**
   * Converts a Cascading Pipe to a Scalding RichPipe. This method permits implicit conversions from
   * Pipe to RichPipe.
   *
   * @param pipe to convert to a RichPipe.
   * @return a RichPipe wrapping the specified Pipe.
   */
  implicit protected final def pipeToRichPipe(pipe: Pipe): RichPipe = Implicits.pipeToRichPipe(pipe)

  /**
   * Converts a Scalding RichPipe to a Cascading Pipe. This method permits implicit conversions from
   * RichPipe to Pipe.
   *
   * @param richPipe to convert to a Pipe.
   * @return the Pipe wrapped by the specified RichPipe.
   */
  implicit protected final def richPipeToPipe(richPipe: RichPipe): Pipe =
      Implicits.richPipeToPipe(richPipe)
}
