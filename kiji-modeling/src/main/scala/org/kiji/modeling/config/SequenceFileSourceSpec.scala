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
 * Configuration necessary to use a sequence file as a data source.
 *
 * @param path to the HDFS location for this file.
 * @param keyField that keys from this sequence file should be bound to.
 * @param valueField that values from this sequence file should be bound to.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class SequenceFileSourceSpec(
    path: String,
    keyField: Option[String],
    valueField: Option[String])
    extends InputSpec
    with OutputSpec
