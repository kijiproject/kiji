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
 * Encapsulates both the computation and the data (addressed by field names) required for the
 * Extract phase of a model workflow
 *
 * @param fields required to perform the Extract phase of a model workflow.
 * @param fn to run when executing the Extract phase of a model workflow.
 * @tparam I is the type of input data to the Extract phase of this model workflow.
 * @tparam O is the type of output data coming from Extract phase of this model workflow.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ExtractFn[I, O] (
    fields: (Fields, Fields),
    fn: I => O)

/**
 * Represents the Extract phase of a model workflow. Users should extend this trait when defining an
 * Extract phase. The extraction phase of a model workflow is responsible for transforming raw data
 * from a Kiji table into feature vector representation, on a per-entity basis.
 *
 * For example:
 * {{{
 * class MyExtractor extends Extractor {
 *   override val extractFn = extract('textLine -> 'count) { textLine: KijiSlice[String] =>
 *     textLine.getFirstValue.split("""\s+""").size
 *   }
 * }
 * }}}
 *
 * This trait also provides access to outside data sources required for data extraction through the
 * `keyValueStoreSpecs` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
@deprecated(
  message = "The Extractor trait is subject to heavy change or removal in upcoming versions.  It "
    + "is not recommended for use until this deprecation warning is removed.",
  since = "0.1.0")
trait Extractor
    extends KeyValueStores
    with CommandLineArgs
    with FieldConversions
    with TupleConversions {
  /**
   * Used to define the computation required for the Extract phase of the model workflow.
   *
   * @return the computation to perform during the Extract phase of the model workflow.
   */
  def extractFn: ExtractFn[_, _]

  /**
   * Used to specify the computation required for the Extract phase of the model workflow along with
   * the field names of its inputs and the field names to bind to its output.
   *
   * @param fields that identify the input to and the output from this phase.
   * @param fn to run when executing the Extract phase of this model workflow.
   * @param converter to use when validating the input field name/function argument mapping. This
   *     argument is provided automatically and users should not need to specify a converter.
   * @param setter to use when validating the output field name/function return tuple mapping. This
   *     argument is provided automatically and users should not need to specify a setter.
   * @tparam I is the type of input data to the Extract phase of this model workflow.
   * @tparam O is the type of output data coming from Extract phase of this model workflow.
   */
  protected final def extract[I, O]
      (fields: (Fields, Fields))
      (fn: I => O)
      (implicit converter: TupleConverter[I], setter: TupleSetter[O]): ExtractFn[I, O] = {
    converter.assertArityMatches(fields._1)
    setter.assertArityMatches(fields._2)

    ExtractFn(fields, fn)
  }
}
