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

package org.kiji.chopsticks

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.layout.KijiTableLayout

/**
 * Container for the table writer and Kiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Kiji table.
 * This is configured during the sink prepare operation.
 *
 * @param kijiTableWriter is a writer object for this Kiji table.
 * @param kijiLayout is the layout for this Kiji table.
 */
@ApiAudience.Private
@ApiStability.Unstable
private[chopsticks] case class KijiSinkContext (
    kijiTableWriter: KijiTableWriter,
    kijiLayout: KijiTableLayout) {
}
