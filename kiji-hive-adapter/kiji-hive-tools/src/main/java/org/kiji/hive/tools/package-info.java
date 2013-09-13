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
 * Command line tools for the Kiji Hive Adapter that belong in the base Kiji library directory
 * rather than than as a standalone fat jar to be incorporated into Hive.
 *
 * <ul>
 *   <li>{@link CreateHiveTableTool} - CLI tool that reads the layout of a Kiji table and outputs
 *       a CREATE EXTERNAL TABLE statement that is usable as a starting point for Hive.
 * </ul>
 */
package org.kiji.hive.tools;
