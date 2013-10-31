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

import com.twitter.scalding.Args
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConversions

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.KijiJob
import org.kiji.modeling.impl.CommandLineArgs
import org.kiji.modeling.impl.KeyValueStores
import org.kiji.modeling.framework.ModelPipeConversions

// TODO(EXP-167): Update documentation to include an iterative example.
/**
 * Represents the Train phase of a model lifecycle. Users should extend this trait when defining a
 * Train phase. The Train phase of a model lifecycle is responsible for computing the fitted model
 * parameters required by the score phase.
 *
 * To define a custom train phase, override the train method of the trainer trait. This method takes
 * as parameters already configured input and output data sources. Both input and output
 * configurations are stored in a [[org.kiji.modeling.config.ModelEnvironment]]. For
 * example:
 * {{{
 *   class MyTrainer extends Trainer {
 *     override def train(input: Source, output: Source) {
 *       new TrainerJob {
 *         input("inputname")
 *             .map('inputField -> 'intermediateField) { inputColumn: KijiSlice[String] =>
 *               inputColumn.getFirstValue
 *             }
 *             .groupBy('intermediateField) { _.count('count) }
 *             .write(output("outputname"))
 *       }.run
 *     }
 *   }
 * }}}
 *
 * This trait also provides access to outside data sources required for the Train phase through
 * the `keyValueStoreSpecs` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
trait Trainer
    extends KeyValueStores
    with TupleConversions
    with CommandLineArgs {

  /**
   * Override this class to implement a MapReduce flow for the train phase.
   */
  abstract class TrainerJob extends KijiJob(Args(Nil)) with ModelPipeConversions

  /**
   * Used to define the computation required for the Train phase of the model lifecycle.
   *
   * @param input data sources used during the train phase.
   * @param output data sources used during the train phase.
   * @return true if job succeeds, false otherwise.
   */
  def train(
      input: Map[String, Source],
      output: Map[String, Source]): Boolean
}
