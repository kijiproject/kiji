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
 * Avro interfaces for KijiMR.
 *
 * This package is mainly comprised of interfaces that expose avro schemas associated with the
 * input or output of the implementing class.
 *
 * <p>
 *   Analytic functions that write {@link org.apache.avro.mapred.AvroKey} and
 *   {@link org.apache.avro.mapred.AvroValue} objects should implement
 *   {@link org.kiji.mapreduce.avro.AvroKeyWriter} and
 *   {@link org.kiji.mapreduce.avro.AvroValueWriter} respectively. Symmetrically, functions that
 *   read {@link org.apache.avro.mapred.AvroKey} and {@link org.apache.avro.mapred.AvroValue}
 *   objects should implement {@link org.kiji.mapreduce.avro.AvroKeyReader} and
 *   {@link org.kiji.mapreduce.avro.AvroValueReader}.  Commonly this will mean that a Mapper will
 *   implement AvroKeyReader and AvroValueReader and a Reducer will implement AvroKeyWriter and
 *   AvroValueWriter.
 * </p>
 * <p>
 *   AvroMapReader is a read-only implementation of java.util.Map (all methods which would normally
 *   result in a modification to the map will throw UnsupportedOperationException) designed to
 *   properly compare keys in order to allow use of containsKey() and equals() methods.
 * </p>
 *
 */
package org.kiji.mapreduce.avro;
