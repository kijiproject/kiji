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

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Context passed to KijiFreshnessPolicy instances to provide access to outside data.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface PolicyContext {

  /**
   * Get the KijiDataRequest which triggered this freshness check.
   *
   * @return The KijiDataRequest issued by the client for this.
   */
  KijiDataRequest getClientRequest();

  /**
   * Get the name of the column to which the freshness policy is attached.
   *
   * @return The name of the column to which the freshness policy is attached.
   */
  KijiColumnName getAttachedColumn();

  /**
   * Get the Configuration associated with the Kiji instance for this context.
   *
   * @return The Configuration associated with the Kiji instance for this context.
   */
  Configuration getConfiguration();
}
