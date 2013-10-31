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

import cascading.tuple.Fields
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.modeling.impl.CommandLineArgs
import org.kiji.modeling.impl.KeyValueStores

/**
 * Encapsulates both the computation and the data (addressed by field names) required for
 * calculating the distance or error in the evaluate phase.
 *
 * @param fields required to perform the distance calculation in the evaluate phase of a model
 *     lifecycle.
 * @param fn that defines the distance metric.
 * @tparam I is the type of the input data to fn.
 * @tparam O is the type of the output data coming from fn.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class DistanceFn[I, O] private (
    fields: (Fields, Fields),
    fn: I => O)

/**
 * Companion object to DistanceFn to match Scalding syntax and allow pre-canned functions for the
 * distance metric.
 */
object DistanceFn {
  /**
   * Constructor for DistanceFn.
   *
   * @param fields required to perform the distance calculation in the evaluate phase of a model
   *     lifecycle.
   * @param fn that defines the distance metric.
   * @param converter to use when validating the input field name/function argument mapping. This
   *     argument is provided automatically and users should not need to specify a converter.
   * @param setter to use when validating the output field name/function return tuple mapping. This
   *     argument is provided automatically and users should not need to specify a setter.
   * @tparam I is the type of the input data to fn.
   * @tparam O is the type of the output data coming from fn.
   * @return a DistanceFn.
   */
  def apply[I, O]
      (fields: (Fields, Fields))
      (fn: I => O)
      (implicit converter: TupleConverter[I], setter: TupleSetter[O]): DistanceFn[I, O] = {
    converter.assertArityMatches(fields._1)
    setter.assertArityMatches(fields._2)
    DistanceFn(fields, fn)
  }
}

/**
 * Encapsulates both the computation and the data (addressed by field names) required for
 * aggregating the distance or error metric in the evaluate phase.
 *
 * @param fields required to perform the distance aggregation calculation in the evaluate phase of
 *     a model lifecycle.
 * @param initialValue is the identity value required to do the aggregation, e.g. 0 for summation,
 *     1 for multiplication.
 * @param foldFn that defines the aggregation of the distance metric.
 * @param mapFn that defines any post-processing of the distance metric.
 * @tparam I is the type of the input data to fn.
 * @tparam O is the type of the output data coming from fn.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AggregationFn[I, O, T] private (
    fields: (Fields,Fields),
    initialValue: O,
    foldFn: (I,O) => O,
    // TODO(EXP-241): This value parameter should be optional.
    mapFn: O => T)


/**
 * The companion object for AggregationFn to match Scalding syntax and allow pre-canned functions
 * for aggregation operations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object AggregationFn {
  /**
   * Constructor for AggregationFn.
   *
   * @param fields required to perform the distance aggregation calculation in the evaluate phase of
   *     a model lifecycle.
   * @param initialValue is the identity value required to do the aggregation, e.g. 0 for summation,
   *     1 for multiplication.
   * @param foldFn that defines the aggregation of the distance metric.
   * @param mapFn that defines any post-processing of the distance metric.
   * @param converter to use when validating the input field name/function argument mapping. This
   *     argument is provided automatically and users should not need to specify a converter.
   * @param setter to use when validating the output field name/function return tuple mapping. This
   *     argument is provided automatically and users should not need to specify a setter.
   * @tparam I is the type of the input data to fn.
   * @tparam O is the type of the output data coming from fn.
   * @return an AggregationFn.
   */
  def apply[I, O, T]
      (fields: (Fields, Fields))
      (initialValue: O)
      (foldFn: (I, O) => O)
      // TODO(EXP-241): This value parameter should be optional.
      (mapFn: O => T)
      (implicit converter: TupleConverter[I], setter: TupleSetter[O]): AggregationFn[I, O, T] = {
    converter.assertArityMatches(fields._1)
    setter.assertArityMatches(fields._2)
    AggregationFn(fields, initialValue, foldFn, mapFn)
  }
}

/**
 * Represents the Evaluate phase of a model workflow. Users should extend this trait when defining
 * an Evaluate phase. The Evaluation phase of a model workflow is responsible for calculating
 * precision or error of a model.
 *
 * For example:
 * {{{
 * class MyEvaluator extends Evaluator {
 *   override val distanceFn = DistanceFn(('c1, 'c2) -> 'count) {
 *     columns: (KijiSlice[Double], KijiSlice[Double]) => {
 *       abs(columns._1.getFirstValue - columns._2.getFirstValue)
 *     }
 *   }
 *   override val aggregationFn = AggregationFn('count)(0.0) {
 *     (countSoFar: Double, count: Double) => countSoFar + count
 *   } {
 *     total: Double => total / 2.0
 *   }
 * }
 * }}}
 *
 * This trait also provides access to outside data sources required for data evaluation through the
 * `keyValueStoreSpecs` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
trait Evaluator
    extends KeyValueStores
    with CommandLineArgs
    with FieldConversions
    with TupleConversions {
  /**
   * Used to define the computation required for calculating the distance or error metric in the
   * Evaluate phase of the model workflow. This is typically run per record (or row). Example, to
   * compare the difference between a predicted value and the ground truth.
   *
   * @return the distance/error computation to perform during the evaluate phase of the model
   *    lifecycle.
   */
  def distanceFn: DistanceFn[_, _]

  /**
   * Used to define the computation required for aggregating the error metric in the evaluate phase
   * of the model lifecycle, example normalization. Optional. Not specifying this
   * means that no aggregation will be performed.
   *
   * @return the aggregated distance/error.
   */
  def aggregationFn: Option[AggregationFn[_, _, _]] = None
}
