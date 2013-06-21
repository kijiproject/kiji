/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Base interface for tasks that emit (key, value) pairs.
 *
 * Tasks can be mappers, reducers, gatherers, etc.
 *
 * Users wishing to create jobs should extend KijiMapper, KijiReducer, KijiGatherer, etc, rather
 * than directly implementing this interface.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public interface KVOutputJob {
  /** @return the type of keys emitted by this task. */
  Class<?> getOutputKeyClass();

  /** @return the type of values emitted by this task. */
  Class<?> getOutputValueClass();
}
