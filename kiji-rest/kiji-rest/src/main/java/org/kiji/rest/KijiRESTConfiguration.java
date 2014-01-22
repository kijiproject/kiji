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

package org.kiji.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;

import org.hibernate.validator.constraints.NotEmpty;

import org.kiji.rest.config.FresheningConfiguration;

/**
 * The Java object which is deserialized from the YAML configuration file.
 * This parametrizes the KijiRESTService.
 */
public class KijiRESTConfiguration extends Configuration {
  /** String cluster address. */
  @NotEmpty
  @JsonProperty("cluster")
  private String mCluster;

  /** Subconfiguration for freshening. */
  @JsonProperty("freshening")
  private FresheningConfiguration mFresheningConfiguration = new FresheningConfiguration();

  /** Set global CORS support. */
  @JsonProperty("cors")
  private boolean mCORS = false;

  /** @return The cluster address. */
  public final String getClusterURI() {
    return mCluster;
  }

  /** @return The freshening configuration. */
  public FresheningConfiguration getFresheningConfiguration() {
    return mFresheningConfiguration;
  }

  /** @return Is global CORS turned on or off. */
  public boolean getCORS() {
    return mCORS;
  }
}
