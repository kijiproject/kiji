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

import java.util.List;

import com.yammer.dropwizard.config.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * The Java object which is deserialized from the YAML configuration file.
 * This parametrizes the KijiRESTService.
 */
public class KijiRESTConfiguration extends Configuration {
  /** String cluster address. */
  @NotEmpty
  @JsonProperty
  private String cluster;

  /** List of instance names which are visible to REST clients. */
  @NotEmpty
  @JsonProperty
  private List<String> instances;

  /** @return The cluster address. */
  public final String getClusterURI() {
    return cluster;
  }

  /** @return The list of instance names. */
  public final List<String> getInstances() {
    return instances;
  }
}
