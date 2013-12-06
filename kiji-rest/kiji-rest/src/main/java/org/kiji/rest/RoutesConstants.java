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

/**
 * Interface that defines the various routes that are used
 * in the API.
 */
public final class RoutesConstants {

  /**
   * Private constructor to prevent instantiation.
   */
  private RoutesConstants() {
  }
  /** The namespace for api. */
  private static final String API_VERSION = "/v1";

  /**
   * GETs a message containing a list of the available sub-resources.
   * <li>Path: /v1/
   * <li>Handled by:
   * {@link org.kiji.rest.resources.KijiRESTResource#getRoot}
   */
  public static final String API_ENTRY_PATH = API_VERSION;

  /**
   * GETs version information.
   * <li>Path: /v1/version
   * <li>Handled by:
   * {@link org.kiji.rest.resources.KijiRESTResource#getVersion}
   */
  public static final String VERSION_ENDPOINT = "/version";

  /**
   * GETs a list of instances that are available.
   * <li>Path: /v1/instances/
   * <li>Handled by:
   * {@link org.kiji.rest.resources.InstancesResource#getInstanceList}
   */
  public static final String INSTANCES_PATH = API_VERSION + "/instances";

  /**
   * GETs instance level metadata.
   * <li>Path: /v1/instances/{instance}
   * <li>Handled by:
   * {@link org.kiji.rest.resources.InstanceResource#getInstanceMetadata}
   */
  public static final String INSTANCE_PARAMETER = "instance";
  public static final String INSTANCE_PATH = INSTANCES_PATH + "/{" + INSTANCE_PARAMETER + "}";

  /**
   * GETs a list of tables in the specified instance.
   * <li>Path: /v1/instances/{instance}/tables
   * <li>Handled by:
   * {@link org.kiji.rest.resources.TablesResource#getTables}
   */
  public static final String TABLES_PATH = INSTANCE_PATH + "/tables";

  /**
   * GETs the layout of the specified table.
   * <li>Path: /v1/instances/{instance}/tables/{table}
   * <li>Handled by:
   * {@link org.kiji.rest.resources.TableResource#getTable}
   */
  public static final String TABLE_PARAMETER = "table";
  public static final String TABLE_PATH = TABLES_PATH + "/{" + TABLE_PARAMETER + "}";

  /**
   * GETs rows resources.
   * <li>Path: /v1/instances/{instance}/tables/{table}/rows/
   * <li>Handled by:
   * {@link org.kiji.rest.resources.RowsResource#getRows}
   */
  public static final String ROWS_PATH = TABLE_PATH + "/rows";
}
