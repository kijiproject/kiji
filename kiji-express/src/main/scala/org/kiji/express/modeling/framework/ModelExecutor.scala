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

import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.Preparer
import org.kiji.express.modeling.ScoreProducerJobBuilder
import org.kiji.express.modeling.Scorer
import org.kiji.express.modeling.Trainer
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.impl.ModelJobUtils
import org.kiji.express.modeling.impl.ModelJobUtils.PhaseType

/**
 * The ModelExecutor can be used to run valid combinations of the model lifecycle.
 * Build the ModelExecutor by providing it with the appropriate [[org.kiji.express.modeling
 * .config.ModelDefinition]] and [[org.kiji.express.modeling.config.ModelEnvironment]] as follows:
 *
 * val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)
 *
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
 *
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ModelExecutor (
    modelDefinition: ModelDefinition,
    modelEnvironment: ModelEnvironment,
    hadoopConfiguration: Configuration) {
  /** Preparer to use for this model definition. Optional. */
  private[this] val preparer: Option[Preparer] =
      getInstanceForPhaseClass[Preparer](modelDefinition.preparerClass)

  /** Trainer to use for this model definition. Optional. */
  private[this] val trainer: Option[Trainer] =
      getInstanceForPhaseClass[Trainer](modelDefinition.trainerClass)

  /** ScoreExtractor to use for this model definition. This variable must be initialized if scorer
    * is defined.
    */
  private[this] val scoreExtractor: Option[Extractor] =
      getInstanceForPhaseClass[Extractor](modelDefinition.scoreExtractor)

  /** Scorer to use for this model definition. Optional. */
  private[this] val scorer: Option[Scorer] =
      getInstanceForPhaseClass[Scorer](modelDefinition.scorerClass)

  private def getInstanceForPhaseClass[T](classForPhase: Option[java.lang.Class[_ <: T]])
      : Option[T] = {
    classForPhase
      .map {
      cname: Class[_ <: T] => cname.newInstance()
    }
  }

  /**
   * Runs the prepare phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided
   * to this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this
   * when the prepare phase is not defined.
   *
   * @return true if prepare phase succeeds, false otherwise.
   */
  def runPreparer(): Boolean = {
    if (preparer.isEmpty) {
      throw new IllegalArgumentException("A preparer has not been provided in the Model " +
        "Definition")
    }
    val input: Source = ModelJobUtils.inputSpecToSource(modelEnvironment, PhaseType.PREPARE)
    val output: Source = ModelJobUtils.outputSpecToSource(modelEnvironment, PhaseType.PREPARE)
    preparer.get.prepare(input, output)
  }


  /**
   * Runs the train phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided to
   * this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this when the
   * train phase is not defined.
   *
   * @return true if the train phase succeeds, false otherwise.
   */
  def runTrainer(): Boolean = {
    if (trainer.isEmpty) {
      throw new IllegalArgumentException("A trainer has not been provided in the Model " +
        "Definition")
    }
    val input: Source = ModelJobUtils.inputSpecToSource(modelEnvironment, PhaseType.TRAIN)
    val output: Source = ModelJobUtils.outputSpecToSource(modelEnvironment, PhaseType.TRAIN)
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
    (preparer.isEmpty || runPreparer()) &&
        (trainer.isEmpty || runTrainer()) &&
        (scorer.isEmpty || runScorer())
  }
}

/**
 * The companion object to [[org.kiji.express.modeling.framework.ModelExecutor]].
 *
 */
object ModelExecutor {
  /**
   * Factory method for constructing a ModelExecutor.
   *
   * @param modelDefinition which defines the phase classes for this executor.
   * @param modelEnvironment which specifies how to run this executor.
   * @param hadoopConfiguration for this executor. Optional.
   * @return a [[org.kiji.express.modeling.framework.ModelExecutor]] that can run the phases.
   */
  def apply(modelDefinition: ModelDefinition,
      modelEnvironment: ModelEnvironment,
      hadoopConfiguration: Configuration = HBaseConfiguration.create()): ModelExecutor = {
    new ModelExecutor(modelDefinition, modelEnvironment, hadoopConfiguration)
  }
}
