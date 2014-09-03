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

/**
 * Package containing a Maven plugin to start and stop a Bento cluster in a Docker container for
 * use in integration testing.
 *
 * Goals:
 * <ul>
 *   <li>start</li>
 *   <li>stop</li>
 * </ul>
 *
 * Flags:
 * <table>
 *   <thead>
 *     <tr>
 *       <th>Property</th>
 *       <th>Type</th>
 *       <th>Description</th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td><code>bento.name</code></td>
 *       <td><code>string</code></td>
 *       <td>
 *         Name of the bento instance to use. If not specified, a random name will be generated.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.docker.address</code></td>
 *       <td><code>string</code></td>
 *       <td>Address of the docker daemon to use to manage bento instances.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.skip</code></td>
 *       <td><code>boolean</code></td>
 *       <td>Skips all goals of the bento-maven-plugin.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.skip.create</code></td>
 *       <td><code>boolean</code></td>
 *       <td>
 *         Skips creating the bento instance. Must be used in conjunction with an externally created
 *         bento instance specified through the <code>bento.name</code> property.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.skip.start</code></td>
 *       <td><code>boolean</code></td>
 *       <td>
 *         Skips starting the bento instance. Must be used in conjunction with an externally created
 *         and started bento instance specified through the <code>bento.name</code> property.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.skip.stop</code></td>
 *       <td><code>boolean</code></td>
 *       <td>Skips stopping the bento instance.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.skip.rm</code></td>
 *       <td><code>boolean</code></td>
 *       <td>Skips deleting the bento instance.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.conf.dir</code></td>
 *       <td><code>string</code></td>
 *       <td>
 *         Directory to place configuration files in. Defaults to the test-classes so that
 *         configuration files are on the classpath.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.venv</code></td>
 *       <td><code>string</code></td>
 *       <td>
 *         Python venv root to install the bento cluster to. If not specified, a random name will be
 *         generated.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.platform</code></td>
 *       <td><code>string</code></td>
 *       <td>The version of the hadoop/hbase stack to run in the bento cluster.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.pypi.repository</code></td>
 *       <td><code>string</code></td>
 *       <td>Pypi repository to install kiji-bento-cluster from.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.timeout.start</code></td>
 *       <td><code>long</code></td>
 *       <td>Amount of time to wait for the bento cluster to start.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.timeout.stop</code></td>
 *       <td><code>long</code></td>
 *       <td>Amount of time to wait for the bento cluster to stop.</td>
 *     </tr>
 *     <tr>
 *       <td><code>bento.timeout.interval</code></td>
 *       <td><code>long</code></td>
 *       <td>Interval at which to poll the bento cluster's status when starting or stopping it.</td>
 *     </tr>
 *   </tbody>
 * </table>
 */
package org.kiji.maven.plugins;
