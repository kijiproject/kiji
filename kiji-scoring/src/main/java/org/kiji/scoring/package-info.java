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
 * The main package for uses of KijiScoring.  Contains user facing classes necessary to
 * configure and perform real time scoring.
 *
 * <h3>Classes:</h3>
 * <p>
 *   FreshKijiTableReader: Primary interface for performing fresh reads.  Behaves like a regular
 *   KijiTableReader except for the possibility of freshening.
 * </p>
 * <p>
 *   KijiFreshnessManager: Tool for registering, retrieving, and removing freshness policies
 *   from the meta table.
 * </p>
 * <p>
 *   KijiFreshnessPolicy: SPI implemented by the user to perform freshness checks.
 * </p>
 * <p>
 *   PolicyContext: Interface for providing access to request specific contextual information in
 *   KijiFreshnessPolicies.
 * </p>
 * <h3>Packages:</h3>
 * <p>
 *   impl: Contains ApiAudience.Private implementation classes necessary for scoring.
 * </p>
 * <p>
 *   lib: Contains stock implementations of KijiFreshnessPolicies.
 * </p>
 * <p>
 *   tools: Contains command line interface tools for registering and inspecting freshness policies.
 * </p>
 *
 * <h3>Creation of policies and producers:</h3>
 * <p>Each FreshKijiTableReader has its own copy of its producers and policies.</p>
 * <p>Producers and policies are created lazily as needed in response to the first request that
 *   requires their attached columns. The process of instantiation is as follows:
 *   <ol>
 *     <li>The freshness policy object is created using reflection utilities (calling the empty
 *     constructor.)</li>
 *     <li>The freshness policy is initialized by calling
 *     {@link org.kiji.scoring.KijiFreshnessPolicy#deserialize(String)} with the stored state.</li>
 *     <li>The Producer object is created using reflection utilities (calling the empty
 *     constructor).</li>
 *     <li>{@link org.kiji.mapreduce.produce.KijiProducer#getRequiredStores()} is called followed by
 *     {@link org.kiji.scoring.KijiFreshnessPolicy#getRequiredStores()}. If the producer and policy
 *     both name a store with the same key, the policy's has precedence.</li>
 *     <li>The producer is initialized by calling
 *     {@link org.kiji.mapreduce.produce.KijiProducer#setup(org.kiji.mapreduce.KijiContext)}. The
 *     KijiContext has accessed to the registered key-value stores.</li>
 *   </ol>
 *   The producer and policy for a column are saved and re-used until the reader is closed or a
 *   reload indicates that the column's policy has been unregistered or updated.
 * </p>
 * <h3>Other notes:</h3>
 * <p>
 *   {@link org.kiji.mapreduce.produce.ProducerContext} objects passed to
 *   {@link org.kiji.mapreduce.produce.KijiProducer}s used for freshening will ignore
 *   {@link org.kiji.mapreduce.produce.KijiProducer#getOutputColumn()} and instead be configured
 *   to write to the column where the KijiFreshnessPolicy is attached.  In the case of a freshness
 *   policy attached to a map type family, the ProducerContext will be configured to write to any
 *   qualified column within that family.
 * </p>
 */
package org.kiji.scoring;
