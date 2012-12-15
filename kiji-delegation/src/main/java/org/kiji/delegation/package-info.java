/**
 * (c) Copyright 2012 WibiData, Inc.
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
 * A lightweight service loading library based on java.util.ServiceLoader that
 * can be used by Kiji framework modules for dynamically loading different
 * service provider implementations at runtime. This delegates functionality
 * required by "upstream" modules to downstream runtime implementations.
 *
 * <p>The main client class is {@link Lookup}, which provides an API for lookups,
 * as well as factory methods for lookup service implementations.</p>
 */
package org.kiji.delegation;

