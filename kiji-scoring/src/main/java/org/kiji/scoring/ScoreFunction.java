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
package org.kiji.scoring;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * SPI for custom scoring functions. Clients may extend this abstract class to create their own
 * ScoreFunctions or use the {@link org.kiji.scoring.server.ScoringServerScoreFunction} to provide
 * scores via the Kiji ScoringServer.
 *
 * <p>
 *   A ScoreFunction is essentially a function that accepts a KijiRowData as input and returns a
 *   score based on the application of an underlying model to the input row data. This model scoring
 *   takes place in {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)} with the
 *   assistance of a {@link FreshenerContext}. The context provides accessor methods for various
 *   piece of data relevant to scoring at the time a request is made include the KijiDataRequest
 *   which triggered the freshening, the column to which the Freshener is attached, and user defined
 *   configuration parameters.
 * </p>
 * <p>
 *   ScoreFunctions are responsible for serializing their own state using the
 *   {@link #serializeToParameters()} method.
 * </p>
 * <p>
 *   ScoreFunction methods are broken into three categories:
 *   <ul>
 *     <li>Attachment time methods</li>
 *     <li>One-time setup and cleanup methods</li>
 *     <li>Per-request methods</li>
 *   </ul>
 *   Methods in each category are run at different times. Attachment time methods are run during
 *   Freshener attachment only; this includes {@link #serializeToParameters()} and
 *   {@link #getOutputSchemas()}. One-time setup and cleanup methods are run when Fresheners are
 *   constructed during initialization of a FreshKijiTableReader or during a call to
 *   {@link org.kiji.scoring.FreshKijiTableReader#rereadFreshenerRecords()} for setup, and when a
 *   Freshener has been unloaded from a FreshKijiTableReader or the reader is closed for cleanup;
 *   this includes {@link #getRequiredStores(FreshenerGetStoresContext)},
 *   {@link #setup(FreshenerSetupContext)}, and {@link #cleanup(FreshenerSetupContext)}. Per-request
 *   methods are run every time a client calls
 *   {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} if
 *   this ScoreFunction is attached as part of a Freshener applicable to the request. Per-request
 *   methods may run in multiple threads simultaneously, so they must be thread-safe.
 *   {@link #getDataRequest(FreshenerContext)} and {@link #score(org.kiji.schema.KijiRowData,
 *   FreshenerContext)} are per-request methods.
 * </p>
 *
 * @param <T> Type of the return value of {@link #score(org.kiji.schema.KijiRowData,
 *     FreshenerContext)}.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
public abstract class ScoreFunction<T> {

  // Attachment time methods -----------------------------------------------------------------------

  /**
   * Optionally specify the Avro Schema or schemas of the value returned by
   * {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}. These Schemas will be used to
   * verify compatibility between the output value and the column to which the Freshener is
   * attached. If the list is null or empty, no validation will be performed.
   *
   * @return the Avro Schema of all possible return values of
   *     {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}.
   */
  public List<Schema> getOutputSchemas() {
    return Collections.emptyList();
  }

  /**
   * Serialize any state required by this ScoreFunction to a string-string parameters map for
   * storage in a KijiFreshenerRecord. Values returned by this method will be included in the
   * record's parameters map automatically if an instance of this class is used to register the
   * Freshener via {@link KijiFreshnessManager#registerFreshener(String,
   * org.kiji.schema.KijiColumnName, KijiFreshnessPolicy, ScoreFunction, java.util.Map, boolean,
   * boolean)}.
   * <p>
   *   These parameters will be merged with the return value of
   *   {@link org.kiji.scoring.KijiFreshnessPolicy#serializeToParameters()} and manually submitted
   *   parameters during registration. Manually submitted parameters will have the highest priority
   *   when conflicts arise, followed by parameters from the policy, and parameters from this
   *   ScoreFunction will have the lowest priority.
   * </p>
   * <p>
   *   Because parameters serialized with this method will be stored in a map with parameters from
   *   a KijiFreshnessPolicy as well as any request-time parameters, namespacing keys is advised.
   *   The recommended namespacing is to prepend keys with the qualified class name of the
   *   ScoreFunction implementation (e.g. "org.kiji.scoring.lib.ExampleScoreFunction.sample_key").
   * </p>
   * <p>
   *   If this method requires that the object be {@link #setup(FreshenerSetupContext)}, specify
   *   setupClasses = true during registration via
   *   {@link KijiFreshnessManager#registerFreshener(String, org.kiji.schema.KijiColumnName,
   *   KijiFreshnessPolicy, ScoreFunction, java.util.Map, boolean, boolean)} to instruct the
   *   KijiFreshnessManager to setup the policy before serializing it.
   * </p>
   *
   * @return serialized state of this KijiFreshnessPolicy as string-string parameters.
   */
  public Map<String, String> serializeToParameters() {
    return Collections.emptyMap();
  }

  // One-time setup/cleanup methods ----------------------------------------------------------------

  /**
   * <p>
   *   Returns a mapping that specifies the names of all key-value stores that must be loaded to
   *   execute this component, and default {@link KeyValueStore} definitions that can be used if the
   *   user does not specify alternate locations/implementations. It is an error for any of these
   *   default implementations to be null. If you want to defer KeyValueStore definition to runtime,
   *   bind a name to the {@link org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore} instead.
   * <p>
   * <p>
   *   Note that this method returns <em>default</em> mappings from store names to concrete
   *   implementations. Users may override these mappings, e.g. in MapReduce job configuration. You
   *   <em>should not</em> open a store returned by <code>getRequiredStores()</code> directly; you
   *   should look to a <code>Context</code> object or similar mechanism exposed by the Kiji
   *   framework to determine the actual {@link org.kiji.mapreduce.kvstore.KeyValueStoreReader}
   *   instance to use.
   * </p>
   * <p>
   *   This method is part of the one-time setup of this score function.  It will not be called
   *   during calls to {@link FreshKijiTableReader#get(org.kiji.schema.EntityId,
   *   org.kiji.schema.KijiDataRequest)}.
   * </p>
   * <p>
   *   KeyValueStores defined by this method will be available to
   *   <ul>
   *     <li>{@link #setup(FreshenerSetupContext)}</li>
   *     <li>{@link #cleanup(FreshenerSetupContext)}</li>
   *     <li>{@link #getDataRequest(FreshenerContext)}</li>
   *     <li>{@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}</li>
   *     <li>{@link KijiFreshnessPolicy#setup(FreshenerSetupContext)}</li>
   *     <li>{@link KijiFreshnessPolicy#cleanup(FreshenerSetupContext)}</li>
   *     <li>{@link KijiFreshnessPolicy#shouldUseClientDataRequest(FreshenerContext)}</li>
   *     <li>{@link KijiFreshnessPolicy#getDataRequest(FreshenerContext)}</li>
   *     <li>{@link KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData, FreshenerContext)}</li>
   *   </ul>
   * </p>
   * <p>
   *   If this ScoreFunction defines an
   *   {@link org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore}, the paired
   *   KijiFreshnessPolicy must define a concrete KeyValueStore bound to the same name. If a
   *   KeyValueStore defined by this ScoreFunction and one defined by a paired KijiFreshnessPolicy
   *   share a name, the store defined by the freshness policy will take precedence.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this ScoreFunction. Includes the string-string parameters and the column to
   *     which the Freshener is attached.
   * @return a mapping of KeyValueStore names to definitions required by this ScoreFunction.
   * */
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
    return Collections.emptyMap();
  }

  /**
   * Called once to initialize this ScoreFunction before any calls to
   * {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}.
   * <p>
   *   This method is part of the one-time setup of this score function.  It will not be called
   *   during calls to {@link FreshKijiTableReader#get(org.kiji.schema.EntityId,
   *   org.kiji.schema.KijiDataRequest)}.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this ScoreFunction. Includes the string-string parameters, the column to
   *     which the Freshener is attached, and KeyValueStores configured in
   *     {@link KijiFreshnessPolicy#getRequiredStores(FreshenerGetStoresContext)} and
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}.
   * @throws IOException in case of an error setting up.
   */
  public void setup(FreshenerSetupContext context) throws IOException {
    // Nothing may go here because clients may implement setup without calling super.setup().
  }

  /**
   * Called once to clean up this ScoreFunction after it has been unloaded from a
   * FreshKijiTableReader. Will not be called while any thread is running any per-request method.
   * <p>
   *   This method is part of the one-time setup of this score function.  It will not be called
   *   during calls to {@link FreshKijiTableReader#get(org.kiji.schema.EntityId,
   *   org.kiji.schema.KijiDataRequest)}.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this ScoreFunction. Includes the string-string parameters, the column to
   *     which the Freshener is attached, and KeyValueStores configured in
   *     {@link KijiFreshnessPolicy#getRequiredStores(FreshenerGetStoresContext)} and
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}.
   * @throws IOException in case of an error cleaning up.
   */
  public void cleanup(FreshenerSetupContext context) throws IOException {
    // Nothing may go here because clients may implement cleanup without calling super.cleanup().
  }

  // per-request methods ---------------------------------------------------------------------------

  /**
   * Get the KijiDataRequest to use to retrieve the KijiRowData which will be passed to
   * {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}.
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this ScoreFunction. Includes the string-string parameters, the column to
   *     which the Freshener is attached, KeyValueStores configured in
   *     {@link KijiFreshnessPolicy#getRequiredStores(FreshenerGetStoresContext)} and
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}, and the {@link KijiDataRequest}
   *     which triggered the Freshener containing this ScoreFunction to run.
   * @return the KijiDataRequest to use to retrieve the KijiRowData which will be passed to
   *     {@link #score(org.kiji.schema.KijiRowData, FreshenerContext)}.
   * @throws IOException in case of an error getting the data request.
   */
  public abstract KijiDataRequest getDataRequest(FreshenerContext context) throws IOException;

  /**
   * Calculate a score from the provided {@link KijiRowData}. This score will be committed by the
   * FreshKijiTableReader which executes this ScoreFunction.
   *
   * @param dataToScore row data representing inputs to be scored.
   * @param context provides access to various contextual information about the Freshener which
   *     includes this ScoreFunction. Includes the string-string parameters, the column to
   *     which the Freshener is attached, KeyValueStores configured in
   *     {@link KijiFreshnessPolicy#getRequiredStores(FreshenerGetStoresContext)} and
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}, and the {@link KijiDataRequest}
   *     which triggered the Freshener containing this ScoreFunction to run.
   * @return the calculated score.
   * @throws IOException in case of an error calculating the score.
   */
  public abstract T score(KijiRowData dataToScore, FreshenerContext context) throws IOException;
}
