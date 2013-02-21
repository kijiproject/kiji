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
 * <p>
 *   Bulk importers are used for parsing data from input sources into fields which correspond to 
 *   Kiji columns.  Bulk import jobs in Kiji MapReduce can be created using the
 *   <code>KijiBulkImportJobBuilder</code>.  There are two options for importing data
 *   into Kiji tables: using a bulk importer to create HFiles or "putting" individual rows. A bulk
 *   importer is suitable for reading in data from files, whereas putting is suitable for importing
 *   individual rows.  Bulk importers can be invoked using the <code>kiji bulk-import</code> tool.
 *   Generated HFiles from bulk importers can subsequently be loaded using the
 *   <code>kiji bulk-load</code> tool.
 * </p>
 *
 * <h2>Usable bulk importers:</h2>
 * <li>{@link org.kiji.mapreduce.lib.bulkimport.CommonLogBulkImporter} - Common Log bulk 
 *     importer</li>
 * <li>{@link org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter} - CSV (Comma Separated Value)
 *     bulk importer that also processes TSV(Tab Separated Values).</li>
 * <li>{@link org.kiji.mapreduce.lib.bulkimport.JSONBulkImporter} - JSON bulk importer
 *
 * <h2>Related Documentation:</h2>
 * <li>{@link org.kiji.mapreduce.lib.bulkimport.DescribedInputTextBulkImporter} - Base class for
 *     bulk importing of any form of structured text class.  Other bulk importers will inherit
 *     from this including:</li>
 * <li>{@link org.kiji.mapreduce.lib.bulkimport.KijiTableImportDescriptor} - The bulk import
 *     mapping import configuration.</li>
 */

package org.kiji.mapreduce.lib.bulkimport;
