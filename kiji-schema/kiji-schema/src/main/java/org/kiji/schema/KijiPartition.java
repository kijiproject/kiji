/**
 * (c) Copyright 2015 WibiData, Inc.
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

package org.kiji.schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * An object representing a partition of a Kiji table.
 *
 * <p>
 *   A Kiji partition may be used to scan over a contiguous group of rows in a Kiji table in an
 *   efficient manner.
 * </p>
 *
 * <p>
 *   In general, it is not possible to know whether any given Kiji row is contained within a
 *   partition, because of hashing and differences in underlying storage mechanisms. Therefore,
 *   Kiji partitions are typically only useful for performing whole-table scans in parallel.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public interface KijiPartition { }
