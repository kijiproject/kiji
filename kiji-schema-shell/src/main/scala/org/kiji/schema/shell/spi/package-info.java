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

/**
 * Collection of plugin SPIs that allow users to include extensions in
 * the Kiji Schema shell.
 *
 * <p>This package includes the following SPIs:</p>
 * <ul>
 *   <li>{@link org.kiji.schema.shell.spi.ParserPlugin ParserPlugin} - specifies
 *       syntax for new statements the user can execute.</li>
 *   <li>{@link org.kiji.schema.shell.spi.ParserPluginFactory ParserPluginFactory} -
 *       The SPI entry-point that creates <tt>ParserPlugin</tt> instances.</li>
 * </ul>
 *
 * <p>In addition, this package includes test kits that perform baseline checks that
 * your plugin is conformant to the requirements of the SPI:</p>
 * <ul>
 *   <li>{@link org.kiji.schema.shell.spi.ParserPluginTestKit ParserPluginTestKit} -
 *       tests for your <tt>ParserPlugin</tt> and <tt>ParserPluginFactory</tt> implementations.
 *       </li>
 * </ul>
 */
package org.kiji.schema.shell.spi
