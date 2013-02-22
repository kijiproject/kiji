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

/**
 * Producers for KijiMR.
 *
 * <p>
 *   A KijiProducer executes a function over a subset of the columns in a table row and produces
 *   output to be injected back into a column of that row.  Produce jobs which execute the
 *   producer over all rows in the table can be created using the
 *   <code>KijiProduceJobBuilder</code>.  Producers can be invoked using the
 *   <code>kiji produce</code> tool.
 * </p>

 * <h2>Usable producers:</h2>
 * <li>{@link org.kiji.mapreduce.lib.produce.ConfiguredRegexProducer} - extracts data via
 *     regex.</li>
 * <li>{@link org.kiji.mapreduce.lib.produce.IdentityProducer} - copies data from one family or
 *     column to another.</li>
 */
package org.kiji.mapreduce.lib.produce;
