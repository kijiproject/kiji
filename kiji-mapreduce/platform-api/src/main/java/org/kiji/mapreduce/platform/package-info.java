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
 * Internal API used by KijiMR to provide a compatibility layer over
 * different Hadoop backends.
 *
 * <p>Currently supported backends include:/<p>
 * <ul>
 *   <li>{@link CDH4MR1KijiMRBridge} -
 *       CDH4 is based on Hadoop 2.0 and supports the "MapReduce 1.0" (non-YARN) model.</li>
 *   <li><tt>Hadoop1xKijiMRBridge</tt> -
 *       General support for Apache Hadoop 1.0.x-based distributions (Work
 *       in progress; see KIJIMR-170).</li>
 * </ul>
 */
package org.kiji.mapreduce.platform;
