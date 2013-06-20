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

package org.kiji.delegation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * Indicates that the specified field of a class or object should be auto-initialized
 * through a {@link ConfiguredLookup}.
 *
 * <p>This allows you to use runtime-configured dependency injection to specify
 * the implementation, like so:</p>
 *
 * <div><tt><pre>import org.kiji.delegation.AutoLookup;
 * import org.kiji.delegation.Delegation;
 *
 * class Foo {
 *   @AutoLookup
 *   private Bar myBar;
 *
 *   public Foo() {
 *     Delegation.init(this); // Populate AutoLookup fields.
 *     // Use myBar here.
 *   }
 * }</pre></tt></div>
 *
 * <p>The implementation of the <tt>Bar</tt> interface or abstract class will be chosen
 * by consulting the kiji-delegation.properties resource(s) on the classpath. See
 * {@link Delegation#init(Object)} for more information.</p>
 *
 * <p>Fields with non-null values are ignored, to prevent multiple calls to {@link #init}
 * from accidentally overwriting values already in use. You must explicitly null out fields
 * before reinitializing them.</p>
 */
@ApiAudience.Public
@Inheritance.Sealed
@Retention(value=RetentionPolicy.RUNTIME)
@Documented public @interface AutoLookup { }
