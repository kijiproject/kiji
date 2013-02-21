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

/**
 * Mappers for Kiji MapReduce.
 *
 * <h2>Usable mappers:</h2>
 * <li>{@link org.kiji.mapreduce.lib.map.TextFlatteningMapper} - Takes name/text records and
 *     outputs name/flattened-text pairs. Flattened text has new lines replaced with spaces.</li>
 */
package org.kiji.mapreduce.lib.map;
