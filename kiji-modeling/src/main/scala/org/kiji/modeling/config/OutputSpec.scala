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

package org.kiji.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * Represents the configuration for an output data source for a phase of the model lifecycle.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait OutputSpec

/**
 * Configuration necessary to use a Kiji table as a data sink.
 *
 * @param tableUri addressing the Kiji table that this output spec will write from.
 * @param timestampField the tuple field for the timestamp associated with the output.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how output fields are mapped onto columns in a Kiji table.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KijiOutputSpec(
    tableUri: String,
    fieldBindings: Seq[FieldBinding],
    timestampField: Option[String] = None)
    extends OutputSpec

/**
 * Configuration necessary to use a Kiji table column as the output. Used in the score phase.
 *
 * @param tableUri addressing the Kiji table to write to.
 * @param outputColumn specifies the Kiji column for the output of the score phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KijiSingleColumnOutputSpec(
    tableUri: String,
    outputColumn: String)
    extends OutputSpec
