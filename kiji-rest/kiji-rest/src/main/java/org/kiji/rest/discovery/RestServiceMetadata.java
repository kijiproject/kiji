/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.rest.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * A description of a Kiji Rest service.  Will be serialized into JSON and made available in the
 * service discovery framework.
 */
@JsonRootName("kiji_rest_service")
public final class RestServiceMetadata {

  private final String mHostname;
  private final int mPID;

  /**
   * Create a new Rest service metadata object.
   *
   * @param hostname of the host.
   * @param pid of the process.
   */
  @JsonCreator
  public RestServiceMetadata(
      @JsonProperty("hostname") final String hostname,
      @JsonProperty("pid") final int pid
  ) {
    mHostname = hostname;
    mPID = pid;
  }

  /**
   * Returns the REST process pid.
   *
   * @return the process pid.
   */
  @JsonProperty("hostname")
  public String getHostname() {
    return mHostname;
  }

  /**
   * Returns the REST process pid.
   *
   * @return the process pid.
   */
  @JsonProperty("pid")
  public int getPID() {
    return mPID;
  }
}
