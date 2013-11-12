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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.modeling.impl.CommandLineArgs
import org.kiji.modeling.impl.KeyValueStores

/**
 * Encapsulates both the computation and the data (addressed by field names) required for the Score
 * phase of a model workflow
 *
 * @param fields required to perform the Score phase of a model workflow.
 * @param fn to run when executing the Score phase of a model workflow.
 * @tparam I is the type of input data to the Score phase of this model workflow.
 * @tparam O is the type of output data coming from Score phase of this model workflow.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ScoreFn[I, O] (
    fields: Fields,
    fn: I => O)

/**
 * Represents the Score phase of a model workflow. Users should extend this trait when defining a
 * Score phase. The scoring phase of a model workflow is responsible for transforming a feature
 * vector from the Extract phase into a score.
 *
 * For example:
 * {{{
 * class MyScorer extends Scorer {
 *   override def scoreFn = score('textLine -> 'count) { textLine: Seq[Cell[String]] =>
 *     textLine.getFirstValue.split("""\s+""").size
 *   }
 * }
 * }}}
 *
 * This trait also provides access to outside data sources required for scoring through the
 * `keyValueStoreSpecs` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
trait Scorer
    extends KeyValueStores
    with CommandLineArgs
    with FieldConversions
    with TupleConversions {
  /**
   * Used to define the computation required for the Score phase of the model workflow.
   *
   * @return the computation to perform during the Score phase of the model workflow.
   */
  def scoreFn: ScoreFn[_, _]

  /**
   * Used to specify the computation required for the Score phase of the model workflow along with
   * the field names of its inputs.
   *
   * @param fields that identify the input to this phase.
   * @param fn to run when executing the Score phase of this model workflow.
   * @param converter to use when validating the input field name/function argument mapping. This
   *     argument is provided automatically and users should not need to specify a converter.
   * @tparam I is the type of input data to the Score phase of this model workflow.
   * @tparam O is the type of output data coming from Score phase of this model workflow.
   */
  protected final def score[I, O]
      (fields: Fields)
      (fn: I => O)
      (implicit converter: TupleConverter[I]): ScoreFn[I, O] = {
    converter.assertArityMatches(fields)

    ScoreFn(fields, fn)
  }
}
