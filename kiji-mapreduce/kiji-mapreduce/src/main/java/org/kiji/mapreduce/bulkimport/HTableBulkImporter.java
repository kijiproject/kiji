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

package org.kiji.mapreduce.bulkimport;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.HTableReader;
import org.kiji.mapreduce.KijiBulkImporter;

/**
 * Base class for Kiji bulk importers that read from a raw HBase table (HTable).
 *
 * <p>You may extend this class to be used as the --importer flag when you have specified
 * an --input flag of <code>htable:&lt;tablename&gt;</code> in your <code>kiji
 * bulk-import</code> command.</p>
 *
 * <p>Concrete subclass must implement:</p>
 * <ul>
 *   <li>getOutputColumn() - Specify the target column in the Kiji table.</li>
 *   <li>getInputHTableScan() - The Scan descriptor describing the rows/columns you wish
 *     to read from the HTable.</li>
 *   <li>produce() - Called once per row of the input HTable, filled with the data
 *     requested in the Scan.</li>
 * </ul>
 */
@ApiAudience.Public
public abstract class HTableBulkImporter
    extends KijiBulkImporter<ImmutableBytesWritable, Result>
    implements HTableReader {
}
