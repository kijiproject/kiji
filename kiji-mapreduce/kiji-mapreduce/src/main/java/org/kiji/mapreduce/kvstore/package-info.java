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
 * Key-value store APIs.
 *
 * <p>{@link org.kiji.mapreduce.kvstore.KeyValueStore}s are used to provide MapReduce
 * programs and other operators processing Kiji datasets with the ability to join
 * datasets. One data set can be specified as a key-value store using the KeyValueStore
 * API. The program can use a KeyValueStoreReader to look up values associated with keys.
 * These keys are often driven by records of a dataset being processed by MapReduce.</p>
 *
 * <p>KeyValueStores are read-only and generally depend on their backing store
 * being considered immutable for the duration of the program's run.</p>
 *
 * <p>Several KeyValueStore implementations are available in {@link
 * org.kiji.mapreduce.kvstore.lib}. These implementations read from a variety of file
 * formats, or Kiji tables.</p>
 *
 * <p>A Kiji table processor (Mapper, Gatherer, Producer, etc.) can specify a set of
 * key-value stores it requires, by implementing the {@link
 * org.kiji.mapreduce.kvstore.KeyValueStoreClient} class. You can return a set of
 * key-value store names and default implementations.  These implementations may specify a
 * default directory in HDFS where they expect the data set.</p>
 *
 * <p>Implementers of KeyValueStoreClient may find the {@link
 * org.kiji.mapreduce.kvstore.RequiredStores} static class helpful, as it provides
 * convenient constructors for sets of KeyValueStore configurations of varying size.</p>
 *
 * <p>You can override these KeyValueStore configurations when submitting a MapReduce
 * program; use a {@link org.kiji.mapreduce.framework.MapReduceJobBuilder} or related class to
 * specify a different KeyValueStore implementation to use at run-time using the
 * withStore() method.</p>
 *
 * <p>You can also override the entire set of stores associated with a MapReduce
 * job by using a kvstores XML file. This XML file may specify a set of store names
 * bound to different KeyValueStore configurations.</p>
 *
 * <p>MapReduce jobs may specify a dependency on one or more stores. The Context objects
 * provided as arguments to Producers and Gatherers can open stores by name; the {@link
 * org.kiji.mapreduce.kvstore.KeyValueStoreReader} interface provides a
 * backing-store-independent reader API that allows them to access values by key. If you
 * are using MapReduce directly, you can instantiate a {@link
 * org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory} to get access to the
 * KeyValueStoreReaders associated with the current job.</p>
 *
 * <p>You can write your own KeyValueStore implementation as well; for instance, you may
 * want to read key-value pairs from a different file format, or look up values over a
 * network (e.g., in Redis, MongoDB, or elsewhere). To do so, implement the {@link
 * org.kiji.mapreduce.kvstore.KeyValueStore} interface, and provide a related {@link
 * org.kiji.mapreduce.kvstore.KeyValueStoreReader} implementation as well. You can use the
 * {@link org.kiji.mapreduce.kvstore.lib.FileStoreHelper} object to manage files in HDFS
 * that should be sent to your processes via the DistributedCache.</p>
 */
package org.kiji.mapreduce.kvstore;
