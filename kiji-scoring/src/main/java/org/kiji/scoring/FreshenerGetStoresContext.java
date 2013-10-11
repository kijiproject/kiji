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
import org.kiji.schema.KijiColumnName;

/**
 * Interface defining operations available to {@link KijiFreshnessPolicy} and {@link ScoreFunction}
 * getRequiredStores methods.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface FreshenerGetStoresContext {

  /**
   * Get the value of the configuration parameter associated with the given key, or null if none
   * exists.
   *
   * @param key the configuration key for which to retrieve the associated value.
   * @return the value of the configuration parameter associated with the given key, or null if none
   *     exists.
   */
  String getParameter(String key);

  /**
   * Get the configuration parameters for this FreshenerContext. This map will be initially
   * populated with values from KijiFreshenerRecord's 'parameters' field.  The values in this
   * map will be available to both the {@link KijiFreshnessPolicy} and {@link ScoreFunction}.
   *
   * <p>
   *   The map returned by this method is immutable. Any method which attempts to modify it will
   *   throw an exception.
   * </p>
   *
   * @return the configuration parameters for this FreshenerContext.
   */
  Map<String, String> getParameters();

  /**
   * Get the KijiColumnName of the column to which the Freshener serviced by this context is
   * attached.
   *
   * @return the KijiColumnName of the column to which the Freshener serviced by this context is
   *     attached.
   */
  KijiColumnName getAttachedColumn();
}
