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
 * A read-only Hive storage handler for Kiji tables.
 *
 * <h2>This implements the classes necessary for Hive to run over Kiji tables:</h2>
 * <ul>
 *   <li>{@link KijiTableStorageHandler} - Hive storage handler that specifies KijiTableInputFormat
 *       and KijiTableSerde as the requisite components.
 *   <li>{@link KijiTableInputFormat} - Input format that uses mapred.InputFormat(as opposed
 *       to mapreduce.InputFormat) as required by Hive.
 *   <li>{@link KijiTableSerDe} - Read only Deserializer for Hive.
 *   <li>{@link KijiTableInputSplit} - Contains the Kiji table input splits.
 *   <li>{@link KijiTableRecordReader} - Reads the records from Kiji.
 * </ul>
 */
package org.kiji.hive;
