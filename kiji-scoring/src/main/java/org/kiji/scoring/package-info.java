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
 *   {@link org.kiji.scoring.FreshKijiTableReader}: Primary interface for performing fresh reads.
 *   Behaves like a regular KijiTableReader except for the possibility of freshening.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.KijiFreshnessManager}: Tool for registering, retrieving, removing, and
 *   validating Fresheners from the meta table.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.KijiFreshnessPolicy}: SPI implemented by the user to perform freshness
 *   checks.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.ScoreFunction}: SPI implemented by the user to perform score
 *   calculation.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.FreshenerGetStoresContext}: Context object provided to
 *   {@link org.kiji.scoring.KijiFreshnessPolicy#getRequiredStores(FreshenerGetStoresContext)} and
 *   {@link org.kiji.scoring.ScoreFunction#getRequiredStores(FreshenerGetStoresContext)} to provide
 *   access to information relevant to the attachment of the particular Freshener.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.FreshenerSetupContext}: Context object provided to setup and cleanup
 *   methods of {@link org.kiji.scoring.KijiFreshnessPolicy} and
 *   {@link org.kiji.scoring.ScoreFunction} to provide access to information relevant to the
 *   attachment of the particular Freshener. Extends FreshenerGetStoresContext and includes the
 *   ability to open KeyValueStores specified in getRequiredStores methods of a policy or score
 *   function.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.FreshenerContext}: Context object provided to per-request methods of
 *   {@link org.kiji.scoring.KijiFreshnessPolicy} and {@link org.kiji.scoring.ScoreFunction} to
 *   provide access to information relevant to a single execution of a Freshener. Extends
 *   FreshenerSetupContext and includes accessors for per-request data such as the client's data
 *   request.
 * </p>
 * <h3>Packages:</h3>
 * <p>
 *   {@link org.kiji.scoring.impl}: Contains ApiAudience.Private implementation classes necessary
 *   for scoring.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.lib}: Contains stock implementations of KijiFreshnessPolicies. as well
 *   as classes used to run SPIs from other Kiji projects as Fresheners or ScoreFunctions in other
 *   KijiProjects.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.tools}: Contains command line interface tools for registering,
 *   retrieving, removing, and validating Fresheners.
 * </p>
 * <p>
 *   {@link org.kiji.scoring.statistics}: Contains classes representing metrics gathered by Scoring
 *   about the performance of Fresheners.
 * </p>
 *
 * <h3>Creation of KijiFreshnessPolicies and ScoreFunctions:</h3>
 * <p>
 *   Each FreshKijiTableReader has its own copy of its score functions and policies.
 * </p>
 * <p>
 *   Score functions and policies are created proactively on construction of the reader and on
 *   calls to {@link org.kiji.scoring.FreshKijiTableReader#rereadFreshenerRecords()}. The process of
 *   instantiation is as follows:
 *   <ol>
 *     <li>
 *       The freshness policy object is created using reflection utilities (calling the empty
 *     constructor.)
 *     </li>
 *     <li>The score function object is created using reflection utils.</li>
 *     <li>
 *       KeyValueStores from the score function and policy are merged (if there is a name conflict
 *     the policy's KVStore will be visible) into a KeyValueStoreReaderFactory.
 *     </li>
 *     <li>
 *       The policy is {@link org.kiji.scoring.KijiFreshnessPolicy#setup(FreshenerSetupContext)}.
 *     </li>
 *     <li>
 *       The score function is {@link org.kiji.scoring.ScoreFunction#setup(FreshenerSetupContext)}.
 *     </li>
 *     <li>
 *       The policy and score function are cached in the reader so that they can server freshening
 *       requests.
 *     </li>
 *   </ol>
 * </p>
 *
 * <h3>Lifecycle of a freshened request for a policy and score function:</h3>
 * <p>
 *   <ol>
 *     <li>
 *       The policy's
 *       {@link org.kiji.scoring.KijiFreshnessPolicy#shouldUseClientDataRequest(FreshenerContext)}
 *       method is called to determine what KijiRowData it should check for freshness. If this
 *       returns false, the policy's
 *       {@link org.kiji.scoring.KijiFreshnessPolicy#getDataRequest(FreshenerContext)} is called and
 *       the return value is used to fetch a row data to check.
 *     </li>
 *     <li>
 *       The row data selected is checked for freshness according to the policy's
 *       {@link org.kiji.scoring.KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData,
 *       FreshenerContext)} method. If this return true, the freshener returns because the requested
 *       data is already fresh and does not need to be freshened.
 *     </li>
 *     <li>
 *       If isFresh returns false, the score function's
 *       {@link org.kiji.scoring.ScoreFunction#getDataRequest(FreshenerContext)} is called and the
 *       return value is used to fetch a KijiRowData which will be scored.
 *     </li>
 *     <li>
 *       Finally {@link org.kiji.scoring.ScoreFunction#score(org.kiji.schema.KijiRowData,
 *       FreshenerContext)} is called to compute a score which is written to the KijiTable and
 *       returned to the client.
 *     </li>
 *   </ol>
 * </p>
 * <p>
 *   When a FreshKijiTableReader is closed or its
 *   {@link org.kiji.scoring.FreshKijiTableReader#rereadFreshenerRecords()} is called, Fresheners
 *   may be closed. When a Freshener is closed,
 *   {@link org.kiji.scoring.KijiFreshnessPolicy#cleanup(FreshenerSetupContext)} and
 *   {@link org.kiji.scoring.ScoreFunction#cleanup(FreshenerSetupContext)} are called.
 * </p>
 */
package org.kiji.scoring;
