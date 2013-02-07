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
 * Bulk importers for Kiji MapReduce.
 *
 * Bulk importers in Kiji MapReduce can be created using the
 * {@link org.kiji.mapreduce.KijiBulkImportJobBuilder}.  These classes can generally be specified
 * as part of the {@link org.kiji.mapreduce.KijiBulkImportJobBuilder}.
 *
 * <p>
 *   Classes of note:
 * </p>
 * <ul>
 *   <li>{@link org.kiji.mapreduce.lib.bulkimport.DescribedInputTextBulkImporter} - Base class for
 *       bulk importing of any form of structured text class.  Other bulk importers will inherit
 *       from this including:</li>
 *   <li>{@link org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter} - CSV (Comma Separated Value)
 *       bulk importer that also processes TSV(Tab Separated Values).</li>
 * </ul>
 *
 * <p>
 *   Related Documentation:
 * </p>
 * <ul>
 *   <li>{@link org.kiji.mapreduce.bulkimport.KijiTableImportDescriptor} - The bulk import mapping
 *       import configuration.</li>
 * </ul>
 */

package org.kiji.mapreduce.lib.bulkimport;
