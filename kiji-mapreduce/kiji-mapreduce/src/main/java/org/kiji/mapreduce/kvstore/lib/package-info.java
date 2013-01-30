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

/** Key-value store implementations.
 *
 * <p>This package contains a library of {@link org.kiji.mapreduce.kvstore.KeyValueStore}
 * implementations that can access key-value pairs in various resources.</p>
 *
 * <h2>Available KeyValueStores</h2>
 * <p>Several file-backed KeyValueStore implementations provide access to
 * stores in different file formats:</p>
 *
 * <ul>
 *   <li>{@link org.kiji.mapreduce.kvstore.lib.AvroKVRecordKeyValueStore} - Key-Value
 *   pairs specified in Avro records containing two fields, <em>key</em> and
 *   <em>value</em></li>
 *   <li>{@link org.kiji.mapreduce.kvstore.lib.AvroRecordKeyValueStore} - Avro records in
 *   an Avro file, to be indexed by a configurable field of each record.</li>
 *   <li>{@link org.kiji.mapreduce.kvstore.lib.SeqFileKeyValueStore} - Key-Value pairs in
 *   SequenceFiles</li>
 *   <li>{@link org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore} - string key-value
 *   pairs in delimited text files</li>
 * </ul>
 *
 * <p>You can also access a specific column of a Kiji table by the row's entityId
 * using the {@link org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore}.</p>
 *
 * <p>If you want to declare a name binding on a KeyValueStore whose exact configuration cannot
 * be determined before runtime, use the {@link
 * org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore}. It will throw an IOException
 * in its storeToConf() method, ensuring that your MapReduceJobBuilder must call
 * withStore() to override the definition before launching the job.</p>
 *
 * <p>The {@link org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore} is a good default
 * choice when you plan to override the configuration at runtime, but find it acceptable
 * to operate without this information.</p>
 *
 * <p>Implementers of HDFS file-backed KeyValueStores may find the {@link
 * org.kiji.mapreduce.kvstore.lib.FileStoreHelper} object helpful; it will track the
 * configuration of filenames and manage their assignment to the DistributedCache on
 * behalf of your KeyValueStore. Multiple KeyValueStores associated with a job may use
 * FileStoreHelpers to manage their files without concern for interfering with one
 * another.</p>
 *
 * <h2>Configuring KeyValueStores</h2>
 *
 * <p>The KeyValueStore implementations in this package are all configured through an
 * associated Builder object. Each implementation has a <code>builder()</code> method
 * that returns the associated Builder object. You can use this Builder to configure
 * various properties particular to that KeyValueStore implementation. Its <code>build()</code>
 * method either returns a newly-constructed KeyValueStore, or throws an exception if the
 * configuration contains an error.</p>
 *
 * <p>In addition, some KeyValueStores (e.g., {@link
 * org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore}) can also be created through other
 * factory methods, like <code>get()</code>.</p>
 *
 * <p>In your <code>getRequiredStores()</code> method, you may find the {@link
 * org.kiji.mapreduce.kvstore.RequiredStores} class helpful in quickly assembling a set
 * of KeyValueStore binding definitions.</p>
 *
 * <p>Note that each KeyValueStore also has a public default constructor. You should not
 * use this directly. When creating individual KeyValueStores, you should use its
 * associated Builder object.  KeyValueStores are instantiated by MapReduce programs using
 * reflection (through the default constructor), and then configured using
 * <code>initFromConf()</code>. To initialize a set of KeyValueStores from a Configuration
 * yourself, you should use a {@link
 * org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory} object to deserialize all
 * KeyValueStores in the job configuration.</p>
 */
package org.kiji.mapreduce.kvstore.lib;
