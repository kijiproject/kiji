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
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * SPI for freshness policies. Clients may extend this abstract class to create their own freshness
 * policies for their applications or use stock implementations provided in
 * {@link org.kiji.scoring.lib}.
 *
 * <p>
 *   A KijiFreshnessPolicy is essentially a function that accepts a KijiRowData as input and returns
 *   a boolean representing the freshness of that row data.  This check takes place in the method
 *   {@link #isFresh(org.kiji.schema.KijiRowData, org.kiji.scoring.FreshenerContext)} with the
 *   assistance of a {@link FreshenerContext}.  The context provides accessor methods for various
 *   pieces of data relevant to freshness at the time a request is made including the
 *   KijiDataRequest which triggered the freshness check, the column to which the freshness policy
 *   is attached, and user defined configuration parameters.
 * </p>
 * <p>
 *   The KijiRowData passed to <code>isFresh()</code> is the result of either a request issued by a
 *   user if {@link #shouldUseClientDataRequest(FreshenerContext)} is true, or a custom data request
 *   specific to this KijiFreshnessPolicy if {@link #shouldUseClientDataRequest(FreshenerContext)}
 *   is false and {@link #getDataRequest(FreshenerContext)} is not null.
 * </p>
 * <p>
 *   KijiFreshnessPolicies are responsible for serializing their own state using the
 *   {@link #serializeToParameters()} method.
 * </p>
 * <p>
 *   KijiFreshnessPolicy methods are broken up into three categories:
 *   <ul>
 *     <li>Attachment time methods</li>
 *     <li>One-time setup and cleanup methods</li>
 *     <li>Per-request methods</li>
 *   </ul>
 *   Methods in each category are run at different times. Attachment time methods are run during
 *   Freshener attachment only; this includes {@link #serializeToParameters()}. One-time setup and
 *   cleanup methods are run when Fresheners are constructed during initialization of a
 *   FreshKijiTableReader or during a call to
 *   {@link org.kiji.scoring.FreshKijiTableReader#rereadFreshenerRecords()} for setup, and when a
 *   Freshener has been unloaded from a FreshKijiTableReader or the reader is closed for cleanup;
 *   this includes {@link #getRequiredStores(FreshenerGetStoresContext)},
 *   {@link #setup(FreshenerSetupContext)}, and {@link #cleanup(FreshenerSetupContext)}. Per-request
 *   methods are run every time a client calls
 *   {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} if
 *   this policy is attached as part of a Freshener applicable to the request. Per-request methods
 *   may run in multiple threads simultaneously, so they must be thread-safe.
 *   {@link #shouldUseClientDataRequest(FreshenerContext)},
 *   {@link #getDataRequest(FreshenerContext)}, and {@link #isFresh(org.kiji.schema.KijiRowData,
 *   FreshenerContext)} are per-request methods.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
public abstract class KijiFreshnessPolicy {

  /**
   * Empty data request used to indicate that no data is required to perform a freshness check. To
   * use this request, return it from {@link #getDataRequest(FreshenerContext)} and return false
   * from {@link #shouldUseClientDataRequest(FreshenerContext)}.
   */
  protected static final KijiDataRequest EMPTY_REQUEST = KijiDataRequest.builder().build();

  // Attachment time methods -----------------------------------------------------------------------

  /**
   * Serialize any state required by this KijiFreshnessPolicy to a string-string parameters map for
   * storage in a KijiFreshenerRecord. Values returned by this method will be included in the
   * record's parameters map automatically if an instance of this class is used to register the
   * Freshener via {@link KijiFreshnessManager#registerFreshener(String,
   * org.kiji.schema.KijiColumnName, KijiFreshnessPolicy, ScoreFunction, java.util.Map, boolean,
   * boolean)}.
   * <p>
   *   These parameters will be merged with the return value of
   *   {@link org.kiji.scoring.ScoreFunction#serializeToParameters()} and manually submitted
   *   parameters during registration. Manually submitted parameters will have the highest priority
   *   when conflicts arise, followed by parameters from this policy, and parameters from the
   *   ScoreFunction will have the lowest priority.
   * </p>
   * <p>
   *   Because parameters serialized with this method will be stored in a map with parameters from
   *   a ScoreFunction as well as any request-time parameters, name-spacing keys is advised.
   *   The recommended name-spacing is to prepend keys with the qualified class name of the
   *   KijiFreshnessPolicy implementation (e.g. "org.kiji.scoring.lib.ShelfLife.shelf_life").
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
   *   <em>should not</em> open or close a store returned by <code>getRequiredStores()</code>
   *   directly; you should look to a <code>Context</code> object or similar mechanism exposed by
   *   the Kiji framework to determine the actual
   *   {@link org.kiji.mapreduce.kvstore.KeyValueStoreReader} instance to use.
   * </p>
   * <p>
   *   This method is part of the one-time setup of this policy. It will not be called during calls
   *   to {@link FreshKijiTableReader#get(org.kiji.schema.EntityId,
   *   org.kiji.schema.KijiDataRequest)}.
   * </p>
   * <p>
   *   KeyValueStores defined by this method will be available to
   *   <ul>
   *     <li>{@link #setup(FreshenerSetupContext)}</li>
   *     <li>{@link #cleanup(FreshenerSetupContext)}</li>
   *     <li>{@link #shouldUseClientDataRequest(FreshenerContext)}</li>
   *     <li>{@link #getDataRequest(FreshenerContext)}</li>
   *     <li>{@link #isFresh(org.kiji.schema.KijiRowData, FreshenerContext)}</li>
   *     <li>{@link ScoreFunction#setup(FreshenerSetupContext)}</li>
   *     <li>{@link ScoreFunction#cleanup(FreshenerSetupContext)}</li>
   *     <li>{@link ScoreFunction#getDataRequest(FreshenerContext)}</li>
   *     <li>{@link ScoreFunction#score(org.kiji.schema.KijiRowData, FreshenerContext)}</li>
   *   </ul>
   * </p>
   * <p>
   *   If a ScoreFunction paired with this KijiFreshnessPolicy defines an
   *   {@link org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore}, this method must define a
   *   concrete KeyValueStore bound to the same name. If a KeyValueStore defined by this policy and
   *   one defined by a paired ScoreFunction share a name, the store defined by the policy will take
   *   precedence.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters and the column to
   *     which the Freshener is attached.
   * @return a mapping of KeyValueStore names to definitions required by this KijiFreshnessPolicy.
   */
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
    return Collections.emptyMap();
  }

  /**
   * Called once to setup this KijiFreshnessPolicy after
   * {@link #getRequiredStores(FreshenerGetStoresContext)} and before any calls to
   * {@link #isFresh(org.kiji.schema.KijiRowData, FreshenerContext)}.
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters, the column to
   *     which the Freshener is attached and KeyValueStores configured in
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}.
   * @throws IOException in case of an error setting up.
   */
  public void setup(FreshenerSetupContext context) throws IOException {
    // Nothing may go here because clients may implement setup without calling super.setup().
  }

  /**
   * Called once to clean up this policy after it has been unloaded from a FreshKijiTableReader.
   * Will not be called while any thread is running any per-request method.
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters, the column to
   *     which the Freshener is attached and KeyValueStores configured in
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}.
   * @throws IOException in case of an error cleaning up.
   */
  public void cleanup(FreshenerSetupContext context) throws IOException {
    // Nothing may go here because clients may implement cleanup without calling super.cleanup().
  }

  // per-request methods ---------------------------------------------------------------------------

  /**
   * Returns whether this KijiFreshnessPolicy operates on the data requested by the client or data
   * collected using the policy's custom data request.
   *
   * <p>
   *   This method is called once during every
   *   {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)}
   *   request. If this method returns true,
   *   {@link #getDataRequest(org.kiji.scoring.FreshenerContext)} will be called and the returned
   *   {@link KijiDataRequest} will be used to fetch the data passed to
   *   {@link #isFresh(org.kiji.schema.KijiRowData, org.kiji.scoring.FreshenerContext)}. Because
   *   multiple threads may require this object at the same time, this method must be thread safe.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters, the column to
   *     which the Freshener is attached, KeyValueStores configured in
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}, and the {@link KijiDataRequest}
   *     which triggered the Freshener containing this policy to run.
   * @return Whether to use the client's data request or the policy's custom request.
   */
  public boolean shouldUseClientDataRequest(FreshenerContext context) {
    return true;
  }

  /**
   * Custom data request required to fulfill
   * {@link #isFresh(org.kiji.schema.KijiRowData, org.kiji.scoring.FreshenerContext)} if the
   * client's data request is insufficient.  If
   * {@link #shouldUseClientDataRequest(org.kiji.scoring.FreshenerContext)} is true, this method
   * will not be called, so it's implementation may be null.
   * <p>
   *   If {@link #shouldUseClientDataRequest(org.kiji.scoring.FreshenerContext)} is false, this
   *   method will be called during every
   *   {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)}
   *   request. Because multiple threads may require this object at the same time, this method must
   *   be thread safe.
   * </p>
   *
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters, the column to
   *     which the Freshener is attached, KeyValueStores configured in
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}, and the {@link KijiDataRequest}
   *     which triggered the Freshener containing this policy to run.
   * @return The custom data request.
   */
  public KijiDataRequest getDataRequest(FreshenerContext context) {
    return null;
  }

  /**
   * Tests a KijiRowData for freshness according to this policy.
   * <p>
   *   This method will be called during every
   *   {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)}
   *   request. Because multiple threads may require this object at the same time, this method must
   *   be thread safe.
   * </p>
   *
   * @param rowData The KijiRowData to test for freshness.  The result of either
   *     {@link #getDataRequest(org.kiji.scoring.FreshenerContext)} or the client data request if
   *     {@link #shouldUseClientDataRequest(org.kiji.scoring.FreshenerContext)}
   *     is true.
   * @param context provides access to various contextual information about the Freshener which
   *     includes this KijiFreshnessPolicy. Includes the string-string parameters, the column to
   *     which the Freshener is attached, KeyValueStores configured in
   *     {@link #getRequiredStores(FreshenerGetStoresContext)}, and the {@link KijiDataRequest}
   *     which triggered the Freshener containing this policy to run.
   * @return Whether the data is fresh.
   */
  public abstract boolean isFresh(KijiRowData rowData, FreshenerContext context);
}
