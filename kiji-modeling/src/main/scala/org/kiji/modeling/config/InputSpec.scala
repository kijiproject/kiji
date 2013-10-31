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
 * Represents the configuration for an input data source for a phase of the model lifecycle.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait InputSpec

/**
 * Configuration necessary to use a Kiji table as a data source.
 *
 * @param tableUri addressing the Kiji table that this input spec will read from.
 * @param dataRequest describing the input columns required by this input spec.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the extract phase is mapped onto named
 *     input fields.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KijiInputSpec(
    tableUri: String,
    dataRequest: ExpressDataRequest,
    fieldBindings: Seq[FieldBinding])
    extends InputSpec
