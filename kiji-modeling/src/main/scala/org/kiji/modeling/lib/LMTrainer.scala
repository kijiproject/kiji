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

package org.kiji.modeling.lib

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import com.twitter.scalding.Source
import com.twitter.scalding.TextLine
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.Cell
import org.kiji.modeling.Trainer

/**
 * Trainer to perform across-table linear regression. This currently only supports univariate
 * linear regression.
 * It assumes that the data resides in a Kiji table, with one column bound to `'attributes` and one
 * to `'target`. The Model Environment input maps the name "dataset" to this Kiji table.
 * The initial values of theta (parameters) are supplied in a textfile on HDFS, mapped to the name
 * "parameters".
 * At the end of the calculation, the resulting thetas will be stored on HDFS in the output source
 * mapped to "parameters" in the Model Environment.
 * For example,
 * {{{
 *   val modelEnvironment: ModelEnvironment = ModelEnvironment(
 *       name = "lr-train-model-environment",
 *       version = "1.0",
 *       trainEnvironment = Some(TrainEnvironment(
 *         inputSpec = Map(
 *           "dataset" -> KijiInputSpec(
 *               someTableUri.toString,
 *               dataRequest = new ExpressDataRequest(0, Long.MaxValue,
 *                   Seq(new ExpressColumnRequest("family:column1", 1, None),
 *                       new ExpressColumnRequest("family:column2", 1, None))),
 *               fieldBindings = Seq(
 *                   FieldBinding(tupleFieldName = "attributes", storeFieldName = "family:column1"),
 *                   FieldBinding(tupleFieldName = "target", storeFieldName = "family:column2"))
 *           ),
 *           "parameters" -> TextSourceSpec(
 *             path = hdfs/path/to/initial/thetas
 *           )
 *         ),
 *         outputSpec = Map(
 *           "parameters" -> TextSourceSpec(
 *             path = hdfs/path/to/output/directory
 *           )),
 *         keyValueStoreSpecs = Seq()
 *       ))
 * }}}
 * <p>
 *  This class accepts the following optional arguments:
 *  <ol>
 *    <li>learning-rate is the step size in the direction of the gradient for gradient descent</li>
 *    <li>max-iter is the maximum number of iterations the training phase should perform</li>
 *    <li>epsilon is the value of the acceptable error threshold.</li>
 *  </ol>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class LMTrainer() extends Trainer {
  /**
   * This converts the x-values, i.e. the attribute column of an input row into a vector, prepending
   * the value of x_0 = 1.
   *
   * @param attributes contains the x-values from the input table.
   * @return an ordered sequence of double x-values, with x_0 prepended.
   */
  def vectorizeDataPoint(attributes: Seq[Cell[Double]]): IndexedSeq[Double] = {
    // TODO Decide how the input source will look like for data with more than one attribute.
    val vectorizedAttributes  = new ArrayBuffer[Double]
    vectorizedAttributes.append(1.0, attributes.head.datum)
    vectorizedAttributes.toIndexedSeq
  }

  /**
   * Calculates the L2-norm of the partial derivatives.
   *
   * @param partials is an ordered sequence of doubles containing the partial derivatives.
   * @return the L2-norm of vector.
   */
  def calculateError(partials: IndexedSeq[Double]): Double = {
    math.sqrt(partials
        .map(math.pow(_, 2))
        .reduce(_ + _))
  }

  /**
   * Converts the thetas (parameters) from the provided input file into an ordered sequence of
   * doubles.
   *
   * @param thetas is the source containing the currently calculated parameters for linear
   *    regression.
   * @return an ordered sequence of doubles representing the thetas.
   */
  def vectorizeParameters(thetas: TextLine): (IndexedSeq[Double], IndexedSeq[Double]) = {
    val resultTuple: (Stream[Double], Stream[Double]) = thetas.readAtSubmitter[(Long, String)]
        .map((entry: (Long, String)) => {
          val (lineNum, line) = entry
          val lineComponents = line.split("\\s+")
          (lineComponents(0).toInt, lineComponents(1).toDouble, lineComponents(2).toDouble)
        })
        .sortWith((a,b) => a._1.compareTo(b._1) < 0)
        .map(paramTuple => (paramTuple._2, paramTuple._3))
        .unzip
    (resultTuple._1.toIndexedSeq, resultTuple._2.toIndexedSeq)
  }

  class LMJob (
      input: Map[String, Source],
      output: Map[String, Source],
      val parameters:IndexedSeq[Double],
      numFeatures:Int = 1,
      val learningRate: Double = args.optional("learning-rate").getOrElse("0.25").toDouble)
      extends TrainerJob {
    input.getOrElse("dataset", null)
        .mapTo(('attributes, 'target) -> 'gradient) {
          dataPoint: (Seq[Cell[Double]], Seq[Cell[Double]]) => {
            // X
            val attributes: IndexedSeq[Double] = vectorizeDataPoint(dataPoint._1)
            // y
            val target: Double = dataPoint._2.head.datum
            // y - (theta)'(X)
            val delta: Double = target -
                parameters.zip(attributes).map(x => x._1 * x._2).reduce(_ + _)
            // TODO: May need to convert this into a tuple (attributes(0), attributes(1),......)
            attributes.map(x => x * delta)
          }
        }
        // index thetas by position
        .flatMapTo('gradient -> ('index, 'indexedGradient)) { gradient: IndexedSeq[Double] =>
          gradient.zipWithIndex.map{ x =>(x._2, x._1) }
        }
        // calculating new thetas
        .groupBy('index) {
          _.reduce('indexedGradient -> 'totalIndexedGradient) {
            (gradientSoFar : Double, gradient : Double) => gradientSoFar + gradient
          }
        }
        // update old theta depending on learning rate and convert thetas to a string
        // also write the partial derivative for the respective thetas for this iteration (needed
        // to calculate convergence)
        .mapTo(('index, 'totalIndexedGradient) -> 'indexedGradientString) {
          gradientTuple: (Int, Double) => {
            gradientTuple._1.toString  + "\t" + (parameters(gradientTuple._1) +
                (learningRate * gradientTuple._2)).toString + "\t" + (gradientTuple._2).toString
          }
        }
        .write(output.getOrElse("parameters", null))
  }

  /**
   * The outer iterative train method. This is the entry point into the trainer.
   *
   * @param input data sources used during the train phase. For more details, refer to the scaladocs
   *    for [[org.kiji.modeling.lib.LMTrainer]].
   * @param output data sources used during the train phase. For more details, refer to the
   *    scaladocs for [[org.kiji.modeling.lib.LMTrainer]]
   * @return true if job succeeds, false otherwise.
   */
  override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
    val logger: Logger = LoggerFactory.getLogger(classOf[LMTrainer])

    var parameterSource:TextLine = input.getOrElse("parameters", null).asInstanceOf[TextLine]
    var outputSource: TextLine = output.getOrElse("parameters", null).asInstanceOf[TextLine]

    // TODO EXP-203 Accept the values below from Args and change this to an appropriate default.
    val max_iter = args.optional("max-iter").getOrElse("100").toInt
    val epsilon = args.optional("epsilon").getOrElse("0.001").toDouble

    var dist: Double = Double.MaxValue
    breakable {
      for (index <- 1 to max_iter) {
        val (parameters:IndexedSeq[Double], partialDerivatives: IndexedSeq[Double]) =
            vectorizeParameters(parameterSource)
        logger.debug("parameters: " + parameters)
        dist = calculateError(partialDerivatives)
        logger.debug("error: " + dist)
        if (dist < epsilon) {
          logger.debug("iterations = " + index)
          break()
        }
        new LMJob(input, output, parameters).run
        // Use the newly calculated thetas in the next iteration.
        parameterSource = outputSource
      }
    }
    // TODO report - number of iterations, error, etc.
    true
  }
}
