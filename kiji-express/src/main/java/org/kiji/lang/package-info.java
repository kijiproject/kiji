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
 * Package containing the main implementation classes for the Kiji Scalding adapter. These
 * classes make it possible to use a Kiji table as an input source in a Cascading flow. Of
 * special importance are:
 * <ol>
 *   <ul>{@link KijiTap}</ul>, which allows Kiji tables to be used as a source of data in
 *   Cascading / Scalding flows.
 *   <ul>{@link KijiScheme}</ul>, which converts data from Kiji into Cascading's tuple model.
 * </ol>
 */
package org.kiji.lang;
