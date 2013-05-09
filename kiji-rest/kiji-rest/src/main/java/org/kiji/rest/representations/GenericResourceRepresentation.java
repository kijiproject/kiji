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

package org.kiji.rest.representations;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class used to represent a generic resource that has a name and a corresponding URI
 * used to access this resource.
 *
 */
public class GenericResourceRepresentation {

  @JsonProperty("name")
  private String mName;

  @JsonProperty("uri")
  private String mURI;

  /**
   * Constructs a GenericResourceRepresentation given a name and a URI.
   *
   * @param resourceName is the name of the resource.
   * @param resourceURI is the URI to access this resource. This may or may not include the
   *        resourceName but generally does.
   */
  public GenericResourceRepresentation(String resourceName, String resourceURI) {
    this.mName = resourceName;
    this.mURI = resourceURI;
  }

  /**
   * Default constructor to satisfy Jackson.
   */
  public GenericResourceRepresentation() {
  }

  /**
   * Returns the name of the resource.
   *
   * @return the name of the resource.
   */
  public String getName() {
    return mName;
  }

  /**
   * Returns the URI of the resource.
   *
   * @return the URI of the resource.
   */
  public String getURI() {
    return mURI;
  }
}
