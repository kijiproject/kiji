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

package org.kiji.express.modeling.framework

import com.twitter.scalding.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.modeling.Preparer
import org.kiji.express.modeling.ScoreProducerJobBuilder
import org.kiji.express.modeling.Trainer
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.impl.ModelJobUtils
import org.kiji.express.modeling.impl.ModelJobUtils.PhaseType

/**
 * The ModelExecutor can be used to run valid combinations of the model lifecycle. Build the
 * ModelExecutor by providing it with the appropriate
 * [[org.kiji.express.modeling.config.ModelDefinition]] and
 * [[org.kiji.express.modeling.config.ModelEnvironment]] as follows:
 * {{{
 * val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)
 * }}}
 * You can then run all the defined phases as:
 * {{{
 * modelExecutor.run()
 * }}}
 * You can also run individual phases as:
 * {{{
 * modelExecutor.runPreparer()
 * modelExecutor.runTrainer()
 * modelExecutor.runScorer()
 * }}}
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
final case class ModelExecutor(
    modelDefinition: ModelDefinition,
    modelEnvironment: ModelEnvironment,
    hadoopConfiguration: Configuration = HBaseConfiguration.create()) {
  /**
   * Creates an instance of the specified phase class.
   *
   * @param classForPhase being instantiated.
   * @tparam T is the type of the class being instantiated.
   * @return a new instance of the specified phase class.
   */
  private def newPhaseInstance[T](classForPhase: Option[java.lang.Class[_ <: T]]): Option[T] = {
    classForPhase.map { _.newInstance() }
  }

  /**
   * Runs the prepare phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided
   * to this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this
   * when the prepare phase is not defined.
   *
   * @return true if prepare phase succeeds, false otherwise.
   */
  def runPreparer(): Boolean = {
    val preparer: Option[Preparer] = newPhaseInstance[Preparer](modelDefinition.preparerClass)
    require(preparer.isDefined, "A preparerClass has not been provided in the Model Definition")

    val inputs: Map[String, Source] = ModelJobUtils.inputSpecsToSource(modelEnvironment,
        PhaseType.PREPARE)
    val outputs: Map[String, Source] = ModelJobUtils.outputSpecsToSource(modelEnvironment,
        PhaseType.PREPARE)
    preparer.get.prepare(inputs, outputs)
  }


  /**
   * Runs the train phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided to
   * this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this when the
   * train phase is not defined.
   *
   * @return true if the train phase succeeds, false otherwise.
   */
  def runTrainer(): Boolean = {
    val trainer: Option[Trainer] = newPhaseInstance[Trainer](modelDefinition.trainerClass)
    require(trainer.isDefined, "A trainer has not been provided in the Model Definition.")

    val input: Map[String, Source] = ModelJobUtils.inputSpecsToSource(modelEnvironment,
        PhaseType.TRAIN)
    val output: Map[String, Source] = ModelJobUtils.outputSpecsToSource(modelEnvironment,
        PhaseType.TRAIN)
    trainer.get.train(input, output)
  }

  /**
   * Runs the extract-score phase of the [[org.kiji.express.modeling.config.ModelDefinition]]
   * provided to this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call
   * this when the score phase is not defined.
   *
   * @return true if the score phase succeeds, false otherwise.
   */
  def runScorer(): Boolean = {
    ScoreProducerJobBuilder
        .buildJob(modelDefinition, modelEnvironment, hadoopConfiguration)
        .run()
  }

  /**
   * Runs all the phases defined by the [[org.kiji.express.modeling.config.ModelDefinition]].
   *
   * @return true if all the phases succeed, false otherwise.
   */
  def run(): Boolean = {
    (modelDefinition.preparerClass.isEmpty || runPreparer()) &&
        (modelDefinition.trainerClass.isEmpty || runTrainer()) &&
        (modelDefinition.scorerClass.isEmpty || runScorer())
  }
}
