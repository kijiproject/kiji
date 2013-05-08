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

import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * The interface for freshness policies. Clients may implement this interface to create their own
 * freshness policies for their applications or use stock implementations provided in
 * org.kiji.scoring.lib.
 *
 * A KijiFreshnessPolicy is essentially a function that accepts a KijiRowData as input and returns
 * a boolean representing the freshness of that row data.  This check takes place in the method
 * {@link #isFresh(org.kiji.schema.KijiRowData, PolicyContext)} with the assistance of a
 * {@link PolicyContext}.  The context provides accessor methods for various pieces of data relevant
 * to freshness at the time a request is made including the KijiDataReqeuest which triggered the
 * freshness check, the column to which the freshness policy is attached, and the Hadoop
 * Configuration of the Kiji instance in which that table lives.
 *
 * The KijiRowData passed to <code>isFresh()</code> is the result of either a request issued by a
 * user if {@link #shouldUseClientDataRequest()} is true, or a custom data request specific to this
 * KijiFreshnessPolicy if {@link #shouldUseClientDataRequest()} is false and
 * {@link #getDataRequest()} is not null.
 *
 * KijiFreshnessPolicies are responsible for serializing and deserializing their own state using
 * {@link #serialize()} and {@link #deserialize(String)} methods.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
public interface KijiFreshnessPolicy extends KeyValueStoreClient {
  /**
   * Tests a KijiRowData for freshness according to this policy.
   *
   * @param rowData The KijiRowData to test for freshness.  The result of either
   *   {@link #getDataRequest()} or the client data request if {@link #shouldUseClientDataRequest()}
   *   is true.
   * @param policyContext the PolicyContext object containing information about this execution
   * state.
   * @return Whether the data is fresh.
   */
  boolean isFresh(KijiRowData rowData, PolicyContext policyContext);

  /**
   * Does this freshness policy operate on the client's requested data, or should it use its own
   * custom data request?
   *
   * @return Whether to use the client's data request.
   */
  boolean shouldUseClientDataRequest();

  /**
   * Custom data request required to fulfill
   * {@link #isFresh(org.kiji.schema.KijiRowData, org.kiji.scoring.PolicyContext)} if the
   * client's data request is insufficient.  If {@link #shouldUseClientDataRequest()} is true, this
   * method will not be called, so it's implementation may be null.
   *
   * @return The custom data request.
   */
  KijiDataRequest getDataRequest();

  /** {@inheritDoc} */
  @Override
  Map<String, KeyValueStore<?, ?>> getRequiredStores();

  /**
   * Serializes any state of the freshness policy for storage in a
   * {@link org.kiji.schema.KijiMetaTable}.
   *
   * @return A string representing any required state for this freshness policy.
   */
  String serialize();

  /**
   * Deserializes state from a {@link org.kiji.schema.KijiMetaTable} and initializes this freshness
   * policy with that state.
   * @param policyState Serialized string retrieved from a KijiMetaTable.
   */
  void deserialize(String policyState);
}
